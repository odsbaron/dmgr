from datetime import UTC, date, datetime, time

import pytest

from quant_research.contracts.bar import AssetClass
from quant_research.universe.contracts import (
    UniverseConstructionMode,
    UniverseDefinition,
    UniverseMember,
    UniverseRef,
    UniverseSnapshot,
    UniverseSnapshotSet,
    UniverseSnapshotSetItem,
    UniverseSourceSpec,
    UniverseSourceType,
)


def definition() -> UniverseDefinition:
    return UniverseDefinition(
        universe_id="ashare-research",
        version="v1",
        name="A-share research universe",
        asset_class=AssetClass.EQUITY,
        calendar_id="XSHG_XSHE",
        timezone="Asia/Shanghai",
        selection_cutoff_time=time(9, 30),
        construction_mode=UniverseConstructionMode.IMPORTED_SNAPSHOT,
    )


def source_spec(source_type: UniverseSourceType = UniverseSourceType.CSV) -> UniverseSourceSpec:
    return UniverseSourceSpec(
        source_id="local-universe",
        universe_id="ashare-research",
        universe_version="v1",
        source_type=source_type,
        path=f"universe.{source_type.value.lower()}",
        trading_date=date(2026, 7, 1),
        known_at=datetime(2026, 7, 1, 1, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 6, 30, 7, 0, tzinfo=UTC),
        field_mapping={"instrument_id": "symbol", "weight": "weight"},
    )


def test_definition_hash_is_stable_and_changes_with_semantics():
    first = definition()
    second = definition()

    assert first.definition_hash == second.definition_hash

    changed = UniverseDefinition(
        **{**first.__dict__, "selection_cutoff_time": time(9, 25)}
    )
    assert changed.definition_hash != first.definition_hash


def test_snapshot_hash_is_independent_of_member_order_and_source_format():
    members = (
        UniverseMember("000001.SZ", weight=0.4, source_row_id="1"),
        UniverseMember("600000.SH", weight=0.6, source_row_id="2"),
    )

    csv_snapshot = UniverseSnapshot.create(
        definition(),
        source_spec(UniverseSourceType.CSV),
        members,
        source_file_hash="sha256:csv",
    )
    parquet_snapshot = UniverseSnapshot.create(
        definition(),
        source_spec(UniverseSourceType.PARQUET),
        tuple(reversed(members)),
        source_file_hash="sha256:parquet",
    )

    assert parquet_snapshot.content_hash == csv_snapshot.content_hash
    assert parquet_snapshot.snapshot_id == csv_snapshot.snapshot_id


def test_snapshot_set_and_ref_are_stable():
    items = (
        UniverseSnapshotSetItem(date(2026, 7, 2), "snapshot-2", "sha256:2"),
        UniverseSnapshotSetItem(date(2026, 7, 1), "snapshot-1", "sha256:1"),
    )

    snapshot_set = UniverseSnapshotSet.create(
        universe_id="ashare-research",
        universe_version="v1",
        definition_hash="sha256:def",
        items=items,
        created_at=datetime(2026, 7, 3, tzinfo=UTC),
    )
    rerun = UniverseSnapshotSet.create(
        universe_id="ashare-research",
        universe_version="v1",
        definition_hash="sha256:def",
        items=tuple(reversed(items)),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert snapshot_set.snapshot_set_id == rerun.snapshot_set_id
    assert snapshot_set.snapshot_set_hash == rerun.snapshot_set_hash
    assert UniverseRef.parse(snapshot_set.ref.uri) == snapshot_set.ref


@pytest.mark.parametrize(
    "value",
    [
        "duckdb://curated_market_bar?snapshot_set_id=set-1",
        "duckdb://universe_member",
    ],
)
def test_universe_ref_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        UniverseRef.parse(value)


def test_definition_rejects_invalid_timezone():
    with pytest.raises(ValueError, match="timezone"):
        UniverseDefinition(
            universe_id="u",
            version="v1",
            name="Universe",
            asset_class=AssetClass.EQUITY,
            calendar_id="calendar",
            timezone="Mars/Olympus",
            selection_cutoff_time=time(9, 30),
        )
