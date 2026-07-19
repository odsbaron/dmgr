from datetime import UTC, date, datetime, time
from itertools import count

import pytest

from quant_research.contracts.bar import AssetClass
from quant_research.universe.contracts import (
    UniverseDefinition,
    UniverseImportStatus,
    UniverseRef,
    UniverseSourceSpec,
    UniverseSourceType,
)
from quant_research.universe.duckdb_store import LocalDuckDBUniverseStore, UniverseStoreError
from quant_research.universe.ingestion import UniverseIngestionService
from quant_research.universe.resolver import UniverseResolver


def definition(*, name: str = "A-share research universe") -> UniverseDefinition:
    return UniverseDefinition(
        universe_id="ashare-research",
        version="v1",
        name=name,
        asset_class=AssetClass.EQUITY,
        calendar_id="XSHG_XSHE",
        timezone="Asia/Shanghai",
        selection_cutoff_time=time(9, 30),
    )


def spec(path, trading_date: date) -> UniverseSourceSpec:
    return UniverseSourceSpec(
        source_id=f"local-{trading_date.isoformat()}",
        universe_id="ashare-research",
        universe_version="v1",
        source_type=UniverseSourceType.CSV,
        path=str(path),
        trading_date=trading_date,
        known_at=datetime.combine(trading_date, time(1, 0), tzinfo=UTC),
        source_data_cutoff=datetime.combine(
            date.fromordinal(trading_date.toordinal() - 1), time(7, 0), tzinfo=UTC
        ),
        field_mapping={"instrument_id": "symbol", "trading_date": "trading_date"},
    )


def write_members(path, trading_date: date, symbols: tuple[str, ...]) -> None:
    rows = "".join(f"{symbol},{trading_date.isoformat()}\n" for symbol in symbols)
    path.write_text(f"symbol,trading_date\n{rows}", encoding="utf-8")


def service(store: LocalDuckDBUniverseStore) -> UniverseIngestionService:
    sequence = count(1)
    return UniverseIngestionService(store, run_id_factory=lambda: f"universe-run-{next(sequence)}")


def test_ingestion_commits_and_reuses_identical_daily_snapshot(tmp_path):
    path = tmp_path / "2026-07-01.csv"
    write_members(path, date(2026, 7, 1), ("000001.SZ", "600000.SH"))
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")
    ingestion = service(store)

    first = ingestion.ingest(definition(), spec(path, date(2026, 7, 1)))
    rerun = ingestion.ingest(definition(), spec(path, date(2026, 7, 1)))

    assert first.status == UniverseImportStatus.COMMITTED
    assert first.snapshot_id is not None
    assert first.row_count_member == 2
    assert rerun.reused_existing is True
    assert rerun.import_run_id == first.import_run_id
    assert rerun.snapshot_id == first.snapshot_id
    snapshot = store.get_snapshot(first.snapshot_id)
    assert snapshot is not None
    assert [member.instrument_id for member in snapshot.members] == ["000001.SZ", "600000.SH"]


def test_ingestion_rejects_conflicting_immutable_partition(tmp_path):
    first_path = tmp_path / "first.csv"
    second_path = tmp_path / "second.csv"
    trading_date = date(2026, 7, 1)
    write_members(first_path, trading_date, ("000001.SZ",))
    write_members(second_path, trading_date, ("600000.SH",))
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")
    ingestion = service(store)

    committed = ingestion.ingest(definition(), spec(first_path, trading_date))
    conflict = ingestion.ingest(definition(), spec(second_path, trading_date))

    assert committed.status == UniverseImportStatus.COMMITTED
    assert conflict.status == UniverseImportStatus.FAILED
    assert conflict.error_code == "IMMUTABLE_PARTITION_CONFLICT"
    snapshot = store.find_snapshot("ashare-research", "v1", trading_date)
    assert snapshot is not None
    assert [member.instrument_id for member in snapshot.members] == ["000001.SZ"]


def test_quality_failure_is_persisted_without_snapshot(tmp_path):
    path = tmp_path / "duplicate.csv"
    path.write_text(
        "symbol,trading_date\n000001.SZ,2026-07-01\n000001.SZ,2026-07-01\n",
        encoding="utf-8",
    )
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")

    result = service(store).ingest(definition(), spec(path, date(2026, 7, 1)))

    assert result.status == UniverseImportStatus.FAILED
    assert result.error_code == "QUALITY_GATE_FAILED"
    assert result.snapshot_id is None
    issues = store.list_quality_issues(result.import_run_id)
    assert [issue.issue_code for issue in issues] == ["DUPLICATE_MEMBER"]


def test_snapshot_set_resolves_exact_daily_memberships(tmp_path):
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")
    ingestion = service(store)
    first_date = date(2026, 7, 1)
    second_date = date(2026, 7, 2)
    first_path = tmp_path / "2026-07-01.csv"
    second_path = tmp_path / "2026-07-02.csv"
    write_members(first_path, first_date, ("A", "B"))
    write_members(second_path, second_date, ("B", "C"))
    ingestion.ingest(definition(), spec(first_path, first_date))
    ingestion.ingest(definition(), spec(second_path, second_date))

    snapshot_set = store.create_snapshot_set(
        universe_id="ashare-research",
        universe_version="v1",
        trading_dates=(second_date, first_date),
    )
    resolved = UniverseResolver(store).resolve(snapshot_set.ref)

    assert UniverseRef.parse(snapshot_set.ref.uri) == snapshot_set.ref
    assert resolved.members_by_date == {
        first_date: frozenset({"A", "B"}),
        second_date: frozenset({"B", "C"}),
    }
    assert resolved.instrument_ids == frozenset({"A", "B", "C"})
    assert resolved.snapshot_set_hash == snapshot_set.snapshot_set_hash


def test_snapshot_set_rejects_missing_dates_and_invalid_refs(tmp_path):
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")
    store.register_definition(definition())

    with pytest.raises(UniverseStoreError) as missing:
        store.create_snapshot_set(
            universe_id="ashare-research",
            universe_version="v1",
            trading_dates=(date(2026, 7, 1),),
        )
    assert missing.value.code == "MISSING_SNAPSHOT"

    with pytest.raises(ValueError, match="universe_member"):
        UniverseResolver(store).resolve("duckdb://curated_market_bar?snapshot_set_id=unknown")


def test_definition_registration_rejects_conflicting_content(tmp_path):
    store = LocalDuckDBUniverseStore(tmp_path / "research.duckdb")
    store.register_definition(definition())

    with pytest.raises(UniverseStoreError) as conflict:
        store.register_definition(definition(name="Changed name"))
    assert conflict.value.code == "DEFINITION_CONFLICT"
