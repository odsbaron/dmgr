from pathlib import Path

from quant_research.contracts.bar import Adjustment, Frequency
from quant_research.contracts.import_run import ImportRun, ImportStatus
from quant_research.contracts.quality import QualityReport
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore
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


def make_import_run(import_run_id: str = "run-1") -> ImportRun:
    return ImportRun.create(
        import_run_id=import_run_id,
        dataset_id="fixture-daily",
        source_id="fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="sha256:fixture",
    )


def normalized_bars(import_run_id: str = "run-1"):
    fixture = Path("tests/fixtures/bars_daily.csv")
    spec = daily_spec(fixture)
    normalizer = BarNormalizer(import_run_id=import_run_id)
    return [normalizer.normalize(row, spec) for row in CSVKLineReader().read_rows(spec)]


def test_duckdb_store_commits_bars_and_reads_by_data_ref(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run = make_import_run()
    bars = normalized_bars(run.import_run_id)

    data_ref = store.commit_import(run, bars, QualityReport(run.import_run_id, ()))

    assert data_ref.uri.startswith("duckdb://curated_market_bar?")
    assert data_ref.filters == {
        "dataset_id": "fixture-daily",
        "freq": "1d",
        "adjustment": "NONE",
        "source_run_id": "run-1",
    }

    persisted = store.read_bars(data_ref)
    stored_run = store.get_import_run("run-1")

    assert [bar.close for bar in persisted] == ["10.1", "10.3"]
    assert persisted[0].bar_start_time.isoformat() == "2026-07-01T01:30:00+00:00"
    assert stored_run is not None
    assert stored_run.status == ImportStatus.COMMITTED
    assert stored_run.row_count_raw == 2
    assert stored_run.row_count_curated == 2

    export_path = store.export_bars_to_parquet(data_ref, tmp_path / "exports" / "bars.parquet")
    assert export_path.exists()
    assert export_path.suffix == ".parquet"


def test_duckdb_store_persists_failed_quality_report_without_curated_rows(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run = make_import_run()
    valid = normalized_bars(run.import_run_id)[0]
    invalid = valid
    report = KLineQualityValidator(import_run_id=run.import_run_id).validate([valid, invalid])

    store.fail_import(
        run,
        report,
        error_code="QUALITY_GATE_FAILED",
        error_message="blocking quality errors",
    )

    stored_run = store.get_import_run("run-1")
    issues = store.list_quality_issues("run-1")

    assert stored_run is not None
    assert stored_run.status == ImportStatus.FAILED
    assert stored_run.row_count_curated == 0
    assert stored_run.issue_count == report.issue_count
    assert [issue.issue_code for issue in issues] == ["DUPLICATE_BAR"]


def test_duckdb_store_finds_committed_import_for_idempotent_replay(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run = make_import_run()
    bars = normalized_bars(run.import_run_id)
    store.commit_import(run, bars, QualityReport(run.import_run_id, ()))

    existing = store.find_committed_import(
        dataset_id="fixture-daily",
        source_id="fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="sha256:fixture",
    )

    assert existing is not None
    assert existing.import_run_id == "run-1"
