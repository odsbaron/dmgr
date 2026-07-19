from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from quant_research.contracts.bar import Adjustment, AssetClass, Frequency
from quant_research.contracts.source import SourceType
from quant_research.data.normalize import BarNormalizer
from quant_research.data.partition_contracts import (
    MarketDataPartition,
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)
from quant_research.data.partition_quality import MarketDataPartitionQualityValidator
from quant_research.data.readers.csv_reader import CSVKLineReader
from quant_research.data.readers.parquet_reader import ParquetKLineReader


ROWS = [
    {
        "symbol": "000001.SZ",
        "exchange": "SZSE",
        "datetime": "2026-07-07T09:30:00+08:00",
        "open": 10,
        "high": 10.1,
        "low": 9.9,
        "close": 10.05,
        "volume": 100,
        "turnover": 1005,
    },
    {
        "symbol": "600000.SH",
        "exchange": "SSE",
        "datetime": "2026-07-07T09:30:00+08:00",
        "open": 8,
        "high": 8.1,
        "low": 7.9,
        "close": 8.05,
        "volume": 200,
        "turnover": 1610,
    },
]


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


def source_spec(path: Path, source_type: SourceType) -> MarketDataSourceSpec:
    return MarketDataSourceSpec(
        source_id="fixture",
        dataset_id="ashare-1m",
        dataset_version="v1",
        source_type=source_type,
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


def write_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "symbol,exchange,datetime,open,high,low,close,volume,turnover",
                "000001.SZ,SZSE,2026-07-07T09:30:00+08:00,10,10.1,9.9,10.05,100,1005",
                "600000.SH,SSE,2026-07-07T09:30:00+08:00,8,8.1,7.9,8.05,200,1610",
            ]
        ),
        encoding="utf-8",
    )


def normalized_bars(reader, spec: MarketDataSourceSpec, run_id: str):
    legacy_spec = spec.to_source_spec(definition())
    return tuple(
        BarNormalizer(import_run_id=run_id).normalize(row, legacy_spec)
        for row in reader.read_rows(legacy_spec)
    )


def test_csv_and_parquet_produce_equivalent_partition_content(tmp_path):
    csv_path = tmp_path / "bars.csv"
    parquet_path = tmp_path / "bars.parquet"
    write_csv(csv_path)
    pl.DataFrame(ROWS).write_parquet(parquet_path)
    csv_spec = source_spec(csv_path, SourceType.CSV)
    parquet_spec = source_spec(parquet_path, SourceType.PARQUET)

    csv_bars = normalized_bars(CSVKLineReader(), csv_spec, "csv-run")
    parquet_bars = normalized_bars(ParquetKLineReader(), parquet_spec, "parquet-run")
    csv_partition = MarketDataPartition.create(
        definition(), csv_spec, csv_bars, source_file_hash="sha256:csv"
    )
    parquet_partition = MarketDataPartition.create(
        definition(), parquet_spec, parquet_bars, source_file_hash="sha256:parquet"
    )

    assert [bar.source_row_id for bar in parquet_bars] == ["1", "2"]
    assert csv_partition.content_hash == parquet_partition.content_hash


def test_partition_quality_blocks_empty_and_naive_metadata(tmp_path):
    spec = replace(
        source_spec(tmp_path / "empty.csv", SourceType.CSV),
        known_at=datetime(2026, 7, 7, 8, 0),
        source_data_cutoff=datetime(2026, 7, 7, 7, 0),
    )

    report = MarketDataPartitionQualityValidator("run-1", definition(), spec).validate(())

    assert {issue.issue_code for issue in report.issues} == {
        "EMPTY_PARTITION",
        "NAIVE_KNOWN_AT",
        "NAIVE_SOURCE_DATA_CUTOFF",
    }
    assert report.has_blocking_errors


def test_partition_quality_blocks_duplicate_date_asset_and_cutoff_mismatch(tmp_path):
    csv_path = tmp_path / "bars.csv"
    write_csv(csv_path)
    spec = source_spec(csv_path, SourceType.CSV)
    valid = normalized_bars(CSVKLineReader(), spec, "run-1")[0]
    invalid = replace(
        valid,
        trading_date=date(2026, 7, 8),
        asset_class=AssetClass.FUTURE,
        bar_end_time=datetime(2026, 7, 7, 7, 1, tzinfo=UTC),
        source_row_id="2",
    )

    report = MarketDataPartitionQualityValidator("run-1", definition(), spec).validate(
        (valid, valid, invalid)
    )
    codes = {issue.issue_code for issue in report.issues}

    assert "DUPLICATE_BAR" in codes
    assert "PARTITION_DATE_MISMATCH" in codes
    assert "BAR_ASSET_CLASS_MISMATCH" in codes
    assert "BAR_AFTER_SOURCE_CUTOFF" in codes
