from pathlib import Path

from quant_research.contracts.bar import Adjustment, Frequency
from quant_research.contracts.import_run import ImportStatus
from quant_research.contracts.refs import DataRef
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.ingestion import DataIngestionService


def daily_spec(path: Path, *, strict_mode: bool = True) -> SourceSpec:
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
        strict_mode=strict_mode,
    )


def test_ingestion_service_commits_csv_to_duckdb(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    service = DataIngestionService(store, run_id_factory=lambda: "run-valid")

    result = service.ingest(daily_spec(Path("tests/fixtures/bars_daily.csv")))

    assert result.status == ImportStatus.COMMITTED
    assert result.import_run_id == "run-valid"
    assert result.row_count_raw == 2
    assert result.row_count_curated == 2
    assert result.data_ref is not None
    assert [bar.close for bar in store.read_bars(result.data_ref)] == ["10.1", "10.3"]


def test_ingestion_service_reuses_committed_import_for_same_source_file(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    run_ids = iter(["run-1", "run-2"])
    service = DataIngestionService(store, run_id_factory=lambda: next(run_ids))
    spec = daily_spec(Path("tests/fixtures/bars_daily.csv"))

    first = service.ingest(spec)
    second = service.ingest(spec)

    assert first.import_run_id == "run-1"
    assert second.import_run_id == "run-1"
    assert second.reused_existing
    assert second.data_ref == first.data_ref
    assert store.get_import_run("run-2") is None
    assert len(store.read_bars(first.data_ref)) == 2


def test_ingestion_service_fails_strict_mode_when_quality_gate_blocks(tmp_path):
    fixture = tmp_path / "invalid_daily.csv"
    fixture.write_text(
        "\n".join(
            [
                "symbol,exchange,date,open,high,low,close,volume,turnover",
                "000001.SZ,SZSE,2026-07-01,10,10.2,9.9,10.1,1000,10100",
                "000001.SZ,SZSE,2026-07-01,10,9.8,10.2,10.1,1000,10100",
            ]
        ),
        encoding="utf-8",
    )
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    service = DataIngestionService(store, run_id_factory=lambda: "run-invalid")

    result = service.ingest(daily_spec(fixture, strict_mode=True))

    assert result.status == ImportStatus.FAILED
    assert result.data_ref is None
    assert result.row_count_raw == 2
    assert result.row_count_curated == 0
    assert result.quality_report.has_blocking_errors

    stored_run = store.get_import_run("run-invalid")
    failed_ref = DataRef(
        "curated_market_bar",
        {"dataset_id": "fixture-daily", "freq": "1d", "source_run_id": "run-invalid"},
    )
    assert stored_run is not None
    assert stored_run.status == ImportStatus.FAILED
    assert stored_run.row_count_raw == 2
    assert store.read_bars(failed_ref) == []
