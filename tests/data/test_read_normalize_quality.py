from pathlib import Path

from quant_research.contracts.bar import Adjustment, Frequency
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.normalize import BarNormalizer
from quant_research.data.quality import KLineQualityValidator
from quant_research.data.readers.csv_reader import CSVKLineReader


def daily_spec(path: Path) -> SourceSpec:
    return SourceSpec(
        source_id="fixture_daily",
        dataset_id="fixture-daily",
        source_type=SourceType.CSV,
        path=str(path),
        freq=Frequency.D1,
        timezone="Asia/Shanghai",
        adjustment=Adjustment.NONE,
        field_mapping={
            "symbol": "symbol",
            "exchange": "exchange",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "turnover": "turnover",
        },
        symbol_mapping={},
        calendar_id="cn_stock_simple",
    )


def minute_spec(path: Path) -> SourceSpec:
    spec = daily_spec(path)
    return SourceSpec(
        source_id="fixture_1m",
        dataset_id="fixture-minute",
        source_type=SourceType.CSV,
        path=str(path),
        freq=Frequency.M1,
        timezone="Asia/Shanghai",
        adjustment=Adjustment.NONE,
        field_mapping={**spec.field_mapping, "datetime": "datetime"},
        symbol_mapping={},
        calendar_id="cn_stock_simple",
    )


def test_csv_reader_yields_source_row_ids():
    fixture = Path("tests/fixtures/bars_daily.csv")

    rows = list(CSVKLineReader().read_rows(daily_spec(fixture)))

    assert rows[0].source_row_id == "1"
    assert rows[0].values["symbol"] == "000001.SZ"
    assert len(rows) == 2


def test_normalizer_creates_daily_bar_record():
    fixture = Path("tests/fixtures/bars_daily.csv")
    row = next(CSVKLineReader().read_rows(daily_spec(fixture)))

    bar = BarNormalizer(import_run_id="run-1").normalize(row, daily_spec(fixture))

    assert bar.dataset_id == "fixture-daily"
    assert bar.freq == Frequency.D1
    assert bar.trading_date.isoformat() == "2026-07-01"
    assert bar.close == "10.1"
    assert bar.source_row_id == "1"


def test_normalizer_preserves_minute_timestamp():
    fixture = Path("tests/fixtures/bars_1m.csv")
    row = next(CSVKLineReader().read_rows(minute_spec(fixture)))

    bar = BarNormalizer(import_run_id="run-1").normalize(row, minute_spec(fixture))

    assert bar.freq == Frequency.M1
    assert bar.trading_date.isoformat() == "2026-07-07"
    assert bar.bar_start_time.isoformat() == "2026-07-07T01:30:00+00:00"
    assert bar.bar_end_time.isoformat() == "2026-07-07T01:31:00+00:00"


def test_quality_validator_reports_duplicate_and_invalid_ohlc():
    fixture = Path("tests/fixtures/bars_daily.csv")
    spec = daily_spec(fixture)
    row = next(CSVKLineReader().read_rows(spec))
    normalizer = BarNormalizer(import_run_id="run-1")
    valid = normalizer.normalize(row, spec)
    invalid = normalizer.normalize(
        row.with_values({"high": "9.0", "low": "9.5", "close": "10.2"}),
        spec,
    )

    report = KLineQualityValidator(import_run_id="run-1").validate([valid, valid, invalid])

    codes = [issue.issue_code for issue in report.issues]
    assert "DUPLICATE_BAR" in codes
    assert "INVALID_OHLC" in codes
    assert report.has_blocking_errors
