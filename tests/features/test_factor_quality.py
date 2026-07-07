from datetime import UTC, datetime, timedelta

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureValue
from quant_research.features.quality import (
    FactorQualityAnalyzer,
    QualitySeverity,
    QualityStatus,
)


def spec(
    factor_id: str,
    *,
    output_field: str | None = None,
    max_null_ratio: float = 0.5,
    forward_bars: int = 0,
    causal: bool | None = None,
) -> RegisteredFactor:
    is_causal = forward_bars == 0 if causal is None else causal
    return RegisteredFactor(
        FactorSpec(
            factor_id=factor_id,
            version="1.0.0",
            namespace="price",
            description=f"{factor_id} quality test factor.",
            input_fields=("close",),
            output_fields=(output_field or factor_id,),
            supported_freqs=(Frequency.D1,),
            lookback_bars=1,
            warmup_bars=0,
            compute_mode=ComputeMode.OPERATOR_GRAPH,
            quality_rules={
                "max_null_ratio": max_null_ratio,
                "forward_bars": forward_bars,
                "causal": is_causal,
            },
        ),
        compute=None,
    )


def value(
    *,
    factor_id: str = "ret_1",
    output_field: str = "ret_1",
    index: int,
    value_float: float | None,
    warmup_complete: bool = True,
) -> FeatureValue:
    as_of = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return FeatureValue(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=as_of.isoformat(),
        factor_id=factor_id,
        factor_version="1.0.0",
        output_field=output_field,
        value_float=value_float,
        value_string=None,
        value_kind="null" if value_float is None else "float",
        warmup_complete=warmup_complete,
        quality_flags=(),
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        created_at=as_of.isoformat(),
    )


def metric(report, factor_id: str, output_field: str, metric_name: str):
    return next(
        item
        for item in report.metrics
        if item.factor_id == factor_id
        and item.output_field == output_field
        and item.metric_name == metric_name
    )


def test_quality_analyzer_computes_null_ratio_and_warmup_count():
    values = [
        value(index=0, value_float=None, warmup_complete=False),
        value(index=1, value_float=0.01, warmup_complete=True),
        value(index=2, value_float=0.02, warmup_complete=True),
    ]

    report = FactorQualityAnalyzer().analyze(values, (spec("ret_1"),))

    assert metric(report, "ret_1", "ret_1", "row_count").metric_value == 3
    assert metric(report, "ret_1", "ret_1", "null_ratio").metric_value == 1 / 3
    assert metric(report, "ret_1", "ret_1", "warmup_incomplete_count").metric_value == 1
    assert report.status == QualityStatus.PASSED


def test_quality_analyzer_marks_null_ratio_over_threshold_as_error():
    values = [
        value(index=0, value_float=None),
        value(index=1, value_float=None),
        value(index=2, value_float=0.02),
    ]

    report = FactorQualityAnalyzer().analyze(
        values,
        (spec("ret_1", max_null_ratio=0.5),),
    )

    null_ratio = metric(report, "ret_1", "ret_1", "null_ratio")
    assert null_ratio.metric_value == 2 / 3
    assert null_ratio.severity == QualitySeverity.ERROR
    assert report.status == QualityStatus.FAILED


def test_quality_analyzer_counts_forward_bars_as_future_leakage_error():
    values = [
        value(factor_id="forward_ret_1", output_field="forward_ret_1", index=0, value_float=0.01),
        value(factor_id="forward_ret_1", output_field="forward_ret_1", index=1, value_float=0.02),
    ]

    report = FactorQualityAnalyzer().analyze(
        values,
        (spec("forward_ret_1", forward_bars=1),),
    )

    leakage = metric(report, "forward_ret_1", "forward_ret_1", "future_leakage_count")
    assert leakage.metric_value == 2
    assert leakage.severity == QualitySeverity.ERROR
    assert leakage.metric_json["check_level"] == "forward_metadata"
    assert leakage.metric_json["forward_bars"] == 1
    assert leakage.metric_json["causal"] is False
    assert report.status == QualityStatus.FAILED


def test_quality_analyzer_counts_non_causal_factor_as_future_leakage_error():
    values = [
        value(factor_id="forward_label", output_field="forward_label", index=0, value_float=0.01),
        value(factor_id="forward_label", output_field="forward_label", index=1, value_float=0.02),
    ]

    report = FactorQualityAnalyzer().analyze(
        values,
        (spec("forward_label", causal=False),),
    )

    leakage = metric(report, "forward_label", "forward_label", "future_leakage_count")
    assert leakage.metric_value == 2
    assert leakage.severity == QualitySeverity.ERROR
    assert leakage.metric_json["causal"] is False
    assert report.status == QualityStatus.FAILED


def test_quality_analyzer_detects_duplicate_feature_keys():
    values = [
        value(index=0, value_float=0.01),
        value(index=0, value_float=0.01),
    ]

    report = FactorQualityAnalyzer().analyze(values, (spec("ret_1"),))

    duplicate_count = metric(report, "ret_1", "ret_1", "duplicate_key_count")
    assert duplicate_count.metric_value == 1
    assert duplicate_count.severity == QualitySeverity.ERROR
    assert report.status == QualityStatus.FAILED
