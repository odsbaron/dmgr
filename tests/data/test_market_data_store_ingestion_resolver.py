from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, Frequency
from quant_research.contracts.import_run import ImportStatus
from quant_research.contracts.source import SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore, MarketDataStoreError
from quant_research.data.ingestion import ImmutableMarketDataIngestionService
from quant_research.data.partition_contracts import (
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)
from quant_research.data.resolver import MarketDataResolver


def definition() -> MarketDatasetDefinition:
    return MarketDatasetDefinition(
        dataset_id="ashare-1m",
        version="v1",
        name="A-share one-minute bars",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        adjustment=Adjustment.NONE,
        calendar_id="xshg-xshe",
        timezone="Asia/Shanghai",
    )


def write_partition(
    path: Path,
    trading_date: date,
    *,
    symbol: str = "000001.SZ",
    close: str = "10.05",
) -> None:
    path.write_text(
        "\n".join(
            [
                "symbol,exchange,datetime,open,high,low,close,volume,turnover",
                (
                    f"{symbol},SZSE,{trading_date.isoformat()}T09:30:00+08:00,"
                    f"10,10.2,9.9,{close},100,1005"
                ),
            ]
        ),
        encoding="utf-8",
    )


def source_spec(path: Path, trading_date: date, *, source_id: str) -> MarketDataSourceSpec:
    return MarketDataSourceSpec(
        source_id=source_id,
        dataset_id="ashare-1m",
        dataset_version="v1",
        source_type=SourceType.CSV,
        path=str(path),
        trading_date=trading_date,
        known_at=datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC).replace(
            hour=9
        ),
        source_data_cutoff=datetime.combine(
            trading_date,
            datetime.min.time(),
            tzinfo=UTC,
        ).replace(hour=8),
        field_mapping={
            "symbol": "symbol",
            "exchange": "exchange",
            "datetime": "datetime",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "turnover": "turnover",
        },
    )


def test_ingestion_reuses_same_import_and_equivalent_partition(tmp_path):
    path = tmp_path / "bars.csv"
    equivalent_path = tmp_path / "equivalent.csv"
    trading_date = date(2026, 7, 7)
    write_partition(path, trading_date)
    write_partition(equivalent_path, trading_date)
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run_ids = iter(["run-1", "run-2", "run-3"])
    service = ImmutableMarketDataIngestionService(
        store,
        run_id_factory=lambda: next(run_ids),
    )

    first = service.ingest(definition(), source_spec(path, trading_date, source_id="source-1"))
    same_import = service.ingest(
        definition(), source_spec(path, trading_date, source_id="source-1")
    )
    equivalent = service.ingest(
        definition(),
        source_spec(equivalent_path, trading_date, source_id="source-2"),
    )

    assert first.status == ImportStatus.COMMITTED
    assert same_import.import_run_id == "run-1"
    assert same_import.reused_existing
    assert equivalent.import_run_id == "run-3"
    assert equivalent.partition_id == first.partition_id
    assert equivalent.reused_existing
    assert store.get_market_data_import_run("run-2") is None


def test_changed_historical_partition_is_rejected_and_original_is_preserved(tmp_path):
    original_path = tmp_path / "original.csv"
    changed_path = tmp_path / "changed.csv"
    trading_date = date(2026, 7, 7)
    write_partition(original_path, trading_date, close="10.05")
    write_partition(changed_path, trading_date, close="10.15")
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run_ids = iter(["run-original", "run-conflict"])
    service = ImmutableMarketDataIngestionService(
        store,
        run_id_factory=lambda: next(run_ids),
    )
    first = service.ingest(
        definition(),
        source_spec(original_path, trading_date, source_id="source-original"),
    )

    conflict = service.ingest(
        definition(),
        source_spec(changed_path, trading_date, source_id="source-changed"),
    )
    stored = store.find_market_data_partition("ashare-1m", "v1", trading_date)

    assert conflict.status == ImportStatus.FAILED
    assert conflict.error_code == "IMMUTABLE_PARTITION_CONFLICT"
    assert stored is not None
    assert stored.partition_id == first.partition_id
    assert [bar.close for bar in stored.bars] == ["10.05"]
    assert store.get_market_data_import_run("run-conflict").status == ImportStatus.FAILED


def test_snapshot_set_resolves_and_reads_only_pinned_partitions(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run_ids = iter(["run-1", "run-2"])
    service = ImmutableMarketDataIngestionService(
        store,
        run_id_factory=lambda: next(run_ids),
    )
    dates = (date(2026, 7, 7), date(2026, 7, 8))
    for index, trading_date in enumerate(dates, start=1):
        path = tmp_path / f"bars-{trading_date.isoformat()}.csv"
        write_partition(path, trading_date, symbol=f"00000{index}.SZ")
        service.ingest(
            definition(),
            source_spec(path, trading_date, source_id=f"source-{index}"),
        )

    snapshot_set = store.create_market_data_snapshot_set(
        dataset_id="ashare-1m",
        dataset_version="v1",
        trading_dates=reversed(dates),
    )
    resolved = MarketDataResolver(store).resolve(
        f"duckdb://curated_market_bar?snapshot_set_id={snapshot_set.snapshot_set_id}"
    )
    bars = store.read_bars(resolved.market_data_ref.uri)

    assert resolved.trading_dates == dates
    assert resolved.dataset_version == "v1"
    assert resolved.definition_hash == definition().definition_hash
    assert [bar.trading_date for bar in bars] == list(dates)
    assert [bar.symbol for bar in bars] == ["000001.SZ", "000002.SZ"]


def test_snapshot_set_requires_every_requested_date(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    store.register_market_dataset_definition(definition())

    with pytest.raises(MarketDataStoreError) as exc_info:
        store.create_market_data_snapshot_set(
            dataset_id="ashare-1m",
            dataset_version="v1",
            trading_dates=(date(2026, 7, 7),),
        )

    assert exc_info.value.code == "MISSING_PARTITION"
    assert "2026-07-07" in exc_info.value.message


def test_resolver_rejects_tampered_snapshot_set_hash(tmp_path):
    db_path = tmp_path / "research.duckdb"
    trading_date = date(2026, 7, 7)
    path = tmp_path / "bars.csv"
    write_partition(path, trading_date)
    store = LocalDuckDBStore(db_path)
    service = ImmutableMarketDataIngestionService(
        store,
        run_id_factory=lambda: "run-1",
    )
    service.ingest(definition(), source_spec(path, trading_date, source_id="source-1"))
    snapshot_set = store.create_market_data_snapshot_set(
        dataset_id="ashare-1m",
        dataset_version="v1",
        trading_dates=(trading_date,),
    )
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE market_data_snapshot_set_item
            SET content_hash = 'sha256:tampered'
            WHERE snapshot_set_id = ?
            """,
            [snapshot_set.snapshot_set_id],
        )

    with pytest.raises(MarketDataStoreError) as exc_info:
        MarketDataResolver(store).resolve(snapshot_set.ref)

    assert exc_info.value.code == "PARTITION_HASH_MISMATCH"


def test_definition_conflict_does_not_replace_existing(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    store.register_market_dataset_definition(definition())

    with pytest.raises(MarketDataStoreError) as exc_info:
        store.register_market_dataset_definition(
            replace(definition(), adjustment=Adjustment.FORWARD)
        )

    assert exc_info.value.code == "DEFINITION_CONFLICT"
    assert store.get_market_dataset_definition("ashare-1m", "v1") == definition()


def test_existing_curated_bar_schema_receives_additive_partition_column(tmp_path):
    db_path = tmp_path / "legacy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE curated_market_bar (
                dataset_id VARCHAR, symbol VARCHAR, exchange VARCHAR, asset_class VARCHAR,
                freq VARCHAR, trading_date VARCHAR, bar_start_time VARCHAR,
                bar_end_time VARCHAR, open VARCHAR, high VARCHAR, low VARCHAR,
                close VARCHAR, volume VARCHAR, turnover VARCHAR, adjustment VARCHAR,
                source VARCHAR, source_run_id VARCHAR, source_row_id VARCHAR, raw_ref VARCHAR
            )
            """
        )

    LocalDuckDBStore(db_path)

    with duckdb.connect(str(db_path)) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('curated_market_bar')").fetchall()
        }
    assert "market_data_partition_id" in columns
