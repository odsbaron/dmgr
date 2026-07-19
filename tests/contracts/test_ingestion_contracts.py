import json
from datetime import UTC, date, datetime, timedelta

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun, ImportStatus
from quant_research.contracts.quality import QualityIssue, Severity
from quant_research.contracts.refs import DataRef
from quant_research.contracts.source import SourceSpec, SourceType


def test_source_spec_defines_csv_daily_input():
    spec = SourceSpec(
        source_id="local_csv_fixture_daily",
        dataset_id="fixture-daily",
        source_type=SourceType.CSV,
        path="tests/fixtures/bars_daily.csv",
        freq=Frequency.D1,
        timezone="Asia/Shanghai",
        adjustment=Adjustment.NONE,
        field_mapping={"date": "date", "close": "close"},
        symbol_mapping={},
        calendar_id="cn_stock_simple",
    )

    assert spec.source_type == SourceType.CSV
    assert spec.strict_mode is True
    assert spec.repair_mode is False


def test_bar_record_preserves_event_time_and_decimal_strings():
    bar = BarRecord(
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        exchange="SZSE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 7),
        bar_start_time=datetime(2026, 7, 7, 1, 30, tzinfo=UTC),
        bar_end_time=datetime(2026, 7, 7, 7, 0, tzinfo=UTC),
        open="10.0000",
        high="10.5000",
        low="9.9000",
        close="10.2000",
        volume="100000",
        turnover="1020000.00",
        adjustment=Adjustment.NONE,
        source="fixture",
        source_run_id="run-1",
        source_row_id="1",
        raw_ref=None,
    )

    assert bar.freq == Frequency.D1
    assert bar.close == "10.2000"
    assert bar.bar_start_time < bar.bar_end_time


def test_data_ref_roundtrip_with_filters():
    ref = DataRef("curated_market_bar", {"dataset_id": "demo", "freq": "1d"})

    parsed = DataRef.parse(ref.uri)

    assert parsed.table == "curated_market_bar"
    assert parsed.filters == {"dataset_id": "demo", "freq": "1d"}
    assert parsed.uri == "duckdb://curated_market_bar?dataset_id=demo&freq=1d"


def test_import_run_and_quality_issue_contracts():
    run = ImportRun.create(
        import_run_id="run-1",
        dataset_id="fixture-daily",
        source_id="local_csv_fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="abc123",
    )
    issue = QualityIssue(
        issue_id="issue-1",
        import_run_id=run.import_run_id,
        dataset_id=run.dataset_id,
        symbol="000001.SZ",
        freq=Frequency.D1,
        trading_date=date(2026, 7, 7),
        bar_start_time=None,
        issue_code="INVALID_OHLC",
        severity=Severity.ERROR,
        message="high is below close",
        raw_ref=None,
    )

    assert run.status == ImportStatus.CREATED
    assert issue.is_blocking


@pytest.mark.parametrize(
    ("freq", "duration"),
    [(Frequency.D1, timedelta(hours=5, minutes=30)), (Frequency.M1, timedelta(minutes=1))],
)
def test_bar_record_roundtrip_preserves_daily_and_minute_contracts(freq, duration):
    start = datetime(2026, 7, 7, 1, 30, tzinfo=UTC)
    bar = BarRecord(
        dataset_id="fixture",
        symbol="000001.SZ",
        exchange="SZSE",
        asset_class=AssetClass.EQUITY,
        freq=freq,
        trading_date=date(2026, 7, 7),
        bar_start_time=start,
        bar_end_time=start + duration,
        open="10.0000",
        high="10.5000",
        low="9.9000",
        close="10.2000",
        volume="100000",
        turnover=None,
        adjustment=Adjustment.NONE,
        source="fixture",
        source_run_id="run-1",
        source_row_id="1",
        raw_ref="raw://fixture/run-1/1",
    )

    payload = json.loads(json.dumps(bar.to_dict()))

    assert BarRecord.from_dict(payload) == bar
