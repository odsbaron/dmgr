from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.source import BarTimestampConvention, SourceType
from quant_research.data.partition_contracts import (
    MarketDataPartition,
    MarketDataRef,
    MarketDataSnapshotSet,
    MarketDataSnapshotSetItem,
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)


def definition(
    convention: BarTimestampConvention = BarTimestampConvention.START_TIME,
) -> MarketDatasetDefinition:
    return MarketDatasetDefinition(
        dataset_id="ashare-1m",
        version="v1",
        name="A-share one-minute bars",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        adjustment=Adjustment.NONE,
        calendar_id="xshg-xshe",
        timezone="Asia/Shanghai",
        bar_timestamp_convention=convention,
    )


def source_spec(path: Path) -> MarketDataSourceSpec:
    return MarketDataSourceSpec(
        source_id="fixture",
        dataset_id="ashare-1m",
        dataset_version="v1",
        source_type=SourceType.CSV,
        path=str(path),
        trading_date=date(2026, 7, 7),
        known_at=datetime(2026, 7, 7, 8, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 7, 7, 0, tzinfo=UTC),
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


def bar(*, source_run_id: str = "run-1", close: str = "10.10") -> BarRecord:
    return BarRecord(
        dataset_id="ashare-1m",
        symbol="000001.SZ",
        exchange="SZSE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        trading_date=date(2026, 7, 7),
        bar_start_time=datetime(2026, 7, 7, 1, 30, tzinfo=UTC),
        bar_end_time=datetime(2026, 7, 7, 1, 31, tzinfo=UTC),
        open="10.00",
        high="10.20",
        low="9.90",
        close=close,
        volume="1000.0",
        turnover="10100.00",
        adjustment=Adjustment.NONE,
        source="fixture",
        source_run_id=source_run_id,
        source_row_id="1",
        raw_ref=f"raw://fixture/{source_run_id}/1",
    )


def test_definition_hash_covers_timestamp_convention():
    start = definition(BarTimestampConvention.START_TIME)
    end = definition(BarTimestampConvention.END_TIME)

    assert start.definition_hash == definition().definition_hash
    assert start.definition_hash != end.definition_hash


def test_partition_hash_ignores_provenance_and_canonicalizes_decimals(tmp_path):
    spec = source_spec(tmp_path / "bars.csv")
    first = MarketDataPartition.create(
        definition(),
        spec,
        (bar(source_run_id="run-1", close="10.10"),),
        source_file_hash="sha256:first",
    )
    second = MarketDataPartition.create(
        definition(),
        replace(spec, path=str(tmp_path / "bars.parquet"), source_type=SourceType.PARQUET),
        (bar(source_run_id="run-2", close="10.1"),),
        source_file_hash="sha256:second",
    )

    assert first.content_hash == second.content_hash
    assert first.partition_id == second.partition_id


def test_snapshot_set_and_ref_are_stable():
    items = (
        MarketDataSnapshotSetItem(date(2026, 7, 8), "partition-2", "sha256:two"),
        MarketDataSnapshotSetItem(date(2026, 7, 7), "partition-1", "sha256:one"),
    )

    snapshot_set = MarketDataSnapshotSet.create(
        dataset_id="ashare-1m",
        dataset_version="v1",
        definition_hash="sha256:definition",
        items=items,
    )
    ref = MarketDataRef(snapshot_set.snapshot_set_id)

    assert snapshot_set.items[0].trading_date == date(2026, 7, 7)
    assert MarketDataRef.parse(ref.uri) == ref
    assert ref.uri.startswith("duckdb://curated_market_bar?snapshot_set_id=")


def test_market_data_ref_rejects_mixed_filters():
    with pytest.raises(ValueError, match="requires only snapshot_set_id"):
        MarketDataRef.parse(
            "duckdb://curated_market_bar?snapshot_set_id=set-1&dataset_id=ashare-1m"
        )


def test_source_spec_requires_matching_definition_and_timestamp_mapping(tmp_path):
    spec = source_spec(tmp_path / "bars.csv")

    with pytest.raises(ValueError, match="does not match"):
        spec.to_source_spec(replace(definition(), version="v2"))

    with pytest.raises(ValueError, match="requires datetime"):
        replace(spec, field_mapping={k: v for k, v in spec.field_mapping.items() if k != "datetime"}).to_source_spec(
            definition()
        )
