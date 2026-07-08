from datetime import UTC, datetime, timedelta

import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry
from quant_research.features.leakage import (
    CompareWindowMode,
    CutpointSelectionMode,
    PrefixInvarianceLeakageDetector,
    PrefixProbeConfig,
    prefix_report_to_quality_metrics,
)
from quant_research.features.quality import QualitySeverity


def price_frame() -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0]):
        rows.append(
            {
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": start + timedelta(days=index),
                "close": close,
            }
        )
    return pl.DataFrame(rows).lazy()


def run_config(*factor_ids: str) -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=factor_ids,
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def registry_with_forward_return() -> FactorRegistry:
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="forward_ret_1",
        version="1.0.0",
        namespace="label",
        description="Next bar return label.",
        input_fields=("close",),
        output_fields=("forward_ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.POLARS_EXPR,
    )
    registry.register(
        spec,
        lambda _spec, _config: [
            (pl.col("close").shift(-1).over("symbol") / pl.col("close") - 1.0).alias(
                "forward_ret_1"
            )
        ],
    )
    return registry


def registry_with_causal_return() -> FactorRegistry:
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
    )
    registry.register(spec, op.pct_change(field("close"), periods=1).alias("ret_1"))
    return registry


def test_prefix_detector_flags_forward_return_shift():
    registry = registry_with_forward_return()
    config = run_config("forward_ret_1")
    resolved = tuple(registry.resolve_many(config.factor_ids, freq=config.freq))

    report = PrefixInvarianceLeakageDetector().analyze(
        input_frame=price_frame(),
        config=config,
        runner=PolarsFactorRunner(registry),
        resolved_factors=resolved,
        probe_config=PrefixProbeConfig(
            cutpoint_mode=CutpointSelectionMode.EXPLICIT,
            explicit_cutpoints=("2026-07-03T07:00:00+00:00",),
            compare_window_mode=CompareWindowMode.TAIL_BARS,
            compare_tail_bars=1,
            min_prefix_rows=1,
        ),
    )

    assert report.violation_count == 1
    assert report.checked_cutpoint_count == 1
    assert report.compared_value_count == 1
    assert report.examples[0].factor_id == "forward_ret_1"
    assert report.examples[0].output_field == "forward_ret_1"
    assert report.examples[0].prefix_value is None
    assert report.examples[0].full_value == 13.0 / 12.0 - 1.0


def test_prefix_detector_does_not_flag_causal_return():
    registry = registry_with_causal_return()
    config = run_config("ret_1")
    resolved = tuple(registry.resolve_many(config.factor_ids, freq=config.freq))

    report = PrefixInvarianceLeakageDetector().analyze(
        input_frame=price_frame(),
        config=config,
        runner=PolarsFactorRunner(registry),
        resolved_factors=resolved,
        probe_config=PrefixProbeConfig(
            cutpoint_mode=CutpointSelectionMode.EXPLICIT,
            explicit_cutpoints=("2026-07-03T07:00:00+00:00",),
            compare_window_mode=CompareWindowMode.TAIL_BARS,
            compare_tail_bars=2,
            min_prefix_rows=1,
        ),
    )

    assert report.violation_count == 0
    assert report.checked_cutpoint_count == 1
    assert report.compared_value_count == 2
    assert report.examples == ()


def test_prefix_report_converts_to_quality_metrics():
    registry = registry_with_forward_return()
    config = run_config("forward_ret_1")
    resolved = tuple(registry.resolve_many(config.factor_ids, freq=config.freq))
    report = PrefixInvarianceLeakageDetector().analyze(
        input_frame=price_frame(),
        config=config,
        runner=PolarsFactorRunner(registry),
        resolved_factors=resolved,
        probe_config=PrefixProbeConfig(
            cutpoint_mode=CutpointSelectionMode.EXPLICIT,
            explicit_cutpoints=("2026-07-03T07:00:00+00:00",),
            compare_tail_bars=1,
            min_prefix_rows=1,
        ),
    )

    metrics = prefix_report_to_quality_metrics(
        report,
        factor_id="__prefix_probe__",
        output_field="__all__",
        created_at="2026-07-08T00:00:00+00:00",
    )

    by_name = {metric.metric_name: metric for metric in metrics}
    assert by_name["prefix_invariance_violation_count"].metric_value == 1
    assert by_name["prefix_invariance_violation_count"].severity == QualitySeverity.ERROR
    assert by_name["prefix_probe_cutpoint_count"].metric_value == 1
    assert by_name["prefix_probe_compared_value_count"].metric_value == 1
    assert by_name["prefix_probe_changed_ratio"].metric_value == 1
    assert by_name["prefix_probe_changed_ratio"].severity == QualitySeverity.ERROR
    assert by_name["prefix_invariance_violation_count"].metric_json["check_level"] == "prefix_recompute"
    assert by_name["prefix_invariance_violation_count"].metric_json["cutpoint_mode"] == "explicit"
    assert by_name["prefix_invariance_violation_count"].metric_json["compare_window_mode"] == "tail_bars"
    assert by_name["prefix_invariance_violation_count"].metric_json["cutpoints"] == [
        "2026-07-03T07:00:00+00:00"
    ]
    assert by_name["prefix_invariance_violation_count"].metric_json["examples"][0]["factor_id"] == "forward_ret_1"


def test_prefix_detector_skips_cutpoint_without_min_compare_rows():
    registry = registry_with_forward_return()
    config = run_config("forward_ret_1")
    resolved = tuple(registry.resolve_many(config.factor_ids, freq=config.freq))

    report = PrefixInvarianceLeakageDetector().analyze(
        input_frame=price_frame(),
        config=config,
        runner=PolarsFactorRunner(registry),
        resolved_factors=resolved,
        probe_config=PrefixProbeConfig(
            cutpoint_mode=CutpointSelectionMode.EXPLICIT,
            explicit_cutpoints=("2026-07-03T07:00:00+00:00",),
            compare_tail_bars=1,
            min_prefix_rows=1,
            min_compare_rows=2,
        ),
    )

    assert report.checked_cutpoint_count == 0
    assert report.compared_value_count == 0
    assert report.violation_count == 0
    assert report.warnings[0].code == "insufficient_compare_rows"


def test_prefix_detector_reports_missing_explicit_cutpoint_warning_metric():
    registry = registry_with_causal_return()
    config = run_config("ret_1")
    resolved = tuple(registry.resolve_many(config.factor_ids, freq=config.freq))

    report = PrefixInvarianceLeakageDetector().analyze(
        input_frame=price_frame(),
        config=config,
        runner=PolarsFactorRunner(registry),
        resolved_factors=resolved,
        probe_config=PrefixProbeConfig(
            cutpoint_mode=CutpointSelectionMode.EXPLICIT,
            explicit_cutpoints=("2026-07-10T07:00:00+00:00",),
            min_prefix_rows=1,
        ),
    )

    metrics = prefix_report_to_quality_metrics(
        report,
        created_at="2026-07-08T00:00:00+00:00",
    )
    by_name = {metric.metric_name: metric for metric in metrics}

    assert report.checked_cutpoint_count == 0
    assert report.warnings[0].code == "missing_explicit_cutpoint"
    assert by_name["prefix_probe_warning_count"].metric_value == 1
    assert by_name["prefix_probe_warning_count"].severity == QualitySeverity.WARNING
    assert by_name["prefix_probe_warning_count"].metric_json["warnings"][0]["cutpoint"] == (
        "2026-07-10T07:00:00+00:00"
    )
