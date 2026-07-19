from datetime import UTC, date, datetime, timedelta

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun
from quant_research.contracts.quality import QualityReport
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.features.quality import QualityStatus
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.gates import LabelQualityGate
from quant_research.labels.pipeline import (
    LabelPipeline,
    LabelRunRequest,
    LabelRunStatus,
)
from quant_research.labels.quality import LabelQualityAnalyzer


def bar(close: str, index: int) -> BarRecord:
    timestamp = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=timestamp,
        bar_end_time=timestamp,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1000",
        turnover="10000",
        adjustment=Adjustment.NONE,
        source="csv",
        source_run_id="import-run-1",
        source_row_id=f"row-{index}",
        raw_ref="fixture.csv",
    )


def import_run() -> ImportRun:
    return ImportRun.create(
        import_run_id="import-run-1",
        dataset_id="fixture-daily",
        source_id="fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="sha256:fixture",
    )


def seed_bars(store: LocalDuckDBStore):
    return store.commit_import(
        import_run(),
        [bar("10.0", 0), bar("11.0", 1), bar("12.1", 2)],
        QualityReport("import-run-1", ()),
    )


def request(input_data_ref: str) -> LabelRunRequest:
    return LabelRunRequest(
        label_run_id="label-run-1",
        label_set_id="next_return_v1",
        source_ref=input_data_ref,
        label_id="forward_ret_1",
        label_version="1.0.0",
        forward_bars=1,
    )


def test_label_pipeline_generates_quality_passed_label_run_from_curated_bars(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_store = LocalDuckDBStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    data_ref = seed_bars(data_store)

    result = LabelPipeline(
        data_store=data_store,
        label_store=label_store,
        quality_analyzer=LabelQualityAnalyzer(max_null_ratio=0.5),
    ).run(request(data_ref.uri))

    labels = LabelQualityGate(label_store).read_consumable_labels(result.label_ref)
    assert result.status == LabelRunStatus.COMMITTED
    assert result.quality_status == QualityStatus.PASSED
    assert result.consumable is True
    assert result.row_count_label == 3
    assert result.metric_count > 0
    assert labels[0].value_float == 0.1


def test_label_pipeline_keeps_failed_quality_labels_but_blocks_consumption(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_store = LocalDuckDBStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    data_ref = seed_bars(data_store)

    result = LabelPipeline(
        data_store=data_store,
        label_store=label_store,
        quality_analyzer=LabelQualityAnalyzer(max_null_ratio=0.1),
    ).run(request(data_ref.uri))

    manifest = label_store.get_manifest("label-run-1")
    metrics = label_store.list_quality_metrics("label-run-1")
    assert result.status == LabelRunStatus.QUALITY_FAILED
    assert result.quality_status == QualityStatus.FAILED
    assert result.consumable is False
    assert result.block_reason == "quality_failed"
    assert manifest.quality_status == QualityStatus.FAILED.value
    assert any(metric.metric_name == "null_ratio" for metric in metrics)
