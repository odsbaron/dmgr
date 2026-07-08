from datetime import UTC, date, datetime, timedelta

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun
from quant_research.contracts.quality import QualityReport
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.leakage import CutpointSelectionMode, PrefixProbeConfig
from quant_research.features.quality import FactorQualityAnalyzer, QualitySeverity, QualityStatus
from quant_research.pipeline.contracts import ResearchRunRequest, ResearchRunStatus
from quant_research.pipeline.research import ResearchPipeline


def bar(close: str, index: int, *, symbol: str = "000001.SZ") -> BarRecord:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=start,
        bar_end_time=start,
        open="10.0",
        high="20.0",
        low="1.0",
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
        [
            bar("10.0", 0),
            bar("11.0", 1),
            bar("12.0", 2),
        ],
        QualityReport("import-run-1", ()),
    )


def registry_with_ret_1(*, max_null_ratio: float = 1.0) -> FactorRegistry:
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="ret_1",
        version="1.0.0",
        namespace="price",
        description="One bar historical return.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=2,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
        quality_rules={"max_null_ratio": max_null_ratio},
    )
    registry.register(spec, op.pct_change(field("close"), periods=1).alias("ret_1"))
    return registry


def pipeline(db_path, registry: FactorRegistry) -> ResearchPipeline:
    return ResearchPipeline(
        data_store=LocalDuckDBStore(db_path),
        factor_registry=registry,
        factor_runner=PolarsFactorRunner(registry),
        feature_store=LocalDuckDBFeatureStore(db_path),
        quality_analyzer=FactorQualityAnalyzer(),
    )


def request(input_data_ref: str, **overrides) -> ResearchRunRequest:
    params = {
        "factor_run_id": "factor-run-1",
        "feature_set_id": "basic_price_v1",
        "input_data_ref": input_data_ref,
        "factor_ids": ("ret_1",),
    }
    params.update(overrides)
    return ResearchRunRequest(**params)


def test_research_pipeline_commits_quality_passed_feature_run(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(request(data_ref.uri, symbols=("000001.SZ",)))

    assert result.status == ResearchRunStatus.COMMITTED
    assert result.quality_status == QualityStatus.PASSED
    assert result.consumable is True
    assert result.block_reason is None
    assert result.feature_table_ref is not None
    assert result.snapshot_ref is not None
    assert result.manifest_ref is not None
    assert result.row_count_input == 3
    assert result.row_count_feature == 3
    assert result.row_count_snapshot == 3
    assert result.metric_count > 0

    manifest = service.feature_store.get_manifest("factor-run-1")
    snapshots = service.feature_store.read_snapshot(result.snapshot_ref)

    assert manifest.quality_status == QualityStatus.PASSED.value
    assert snapshots[-1].features["ret_1"] == pytest.approx(12.0 / 11.0 - 1.0)


def test_research_pipeline_keeps_failed_quality_assets_but_blocks_consumption(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1(max_null_ratio=0.0))

    result = service.run(request(data_ref.uri))

    metrics = service.feature_store.list_quality_metrics("factor-run-1")
    null_ratio = [metric for metric in metrics if metric.metric_name == "null_ratio"][0]

    assert result.status == ResearchRunStatus.QUALITY_FAILED
    assert result.quality_status == QualityStatus.FAILED
    assert result.consumable is False
    assert result.block_reason == "quality_failed"
    assert result.snapshot_ref is not None
    assert null_ratio.severity == QualitySeverity.ERROR


def test_research_pipeline_blocks_when_prefix_probe_cannot_run_requested_cutpoint(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(
        request(
            data_ref.uri,
            prefix_probe_config=PrefixProbeConfig(
                cutpoint_mode=CutpointSelectionMode.EXPLICIT,
                explicit_cutpoints=("2026-07-10T07:00:00+00:00",),
                min_prefix_rows=1,
            ),
        )
    )

    warning_count = [
        metric
        for metric in service.feature_store.list_quality_metrics("factor-run-1")
        if metric.metric_name == "prefix_probe_warning_count"
    ][0]

    assert result.status == ResearchRunStatus.QUALITY_FAILED
    assert result.quality_status == QualityStatus.FAILED
    assert result.consumable is False
    assert result.block_reason == "quality_failed"
    assert warning_count.metric_value == 1
    assert warning_count.severity == QualitySeverity.ERROR


def test_research_pipeline_reports_invalid_input_ref_as_pipeline_failure(tmp_path):
    service = pipeline(tmp_path / "research.duckdb", registry_with_ret_1())

    result = service.run(
        request("duckdb://feature_table?dataset_id=fixture-daily&freq=1d")
    )

    assert result.status == ResearchRunStatus.FAILED
    assert result.quality_status == QualityStatus.NOT_RUN
    assert result.consumable is False
    assert result.block_reason == "pipeline_failed"
    assert result.error_step == "parse_input_ref"
    assert result.error_code == "INVALID_INPUT_DATA_REF"
