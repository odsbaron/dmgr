from __future__ import annotations

import random

import pytest

from quant_research.evaluation import (
    EvaluationMetricKind,
    EvaluationMetricStatus,
    FactorEvaluationRequest,
    evaluate_cross_sections,
    evaluation_content_hash,
)
from quant_research.features.contracts import FeatureSnapshot
from quant_research.labels.contracts import LabelValue


AS_OF = "2026-07-01T07:00:00+00:00"


def _request(
    *,
    minimum_cross_section_size: int = 4,
    quantile_count: int = 2,
) -> FactorEvaluationRequest:
    return FactorEvaluationRequest(
        evaluation_run_id="eval-1",
        feature_snapshot_ref="duckdb://feature_snapshot?factor_run_id=factor-run-1",
        label_ref="duckdb://label_table?label_run_id=label-run-1",
        factor_fields=("alpha",),
        label_field="forward_ret_1",
        quantile_count=quantile_count,
        minimum_cross_section_size=minimum_cross_section_size,
    )


def _inputs(
    factor_values: list[float | None],
    label_values: list[float | None],
):
    snapshots = []
    labels = []
    for index, (factor_value, label_value) in enumerate(
        zip(factor_values, label_values, strict=True)
    ):
        symbol = f"{index:06d}.SZ"
        snapshots.append(
            FeatureSnapshot(
                snapshot_id=f"snapshot-{index}",
                feature_set_id="alpha-v1",
                dataset_id="fixture-daily",
                symbol=symbol,
                freq="1d",
                as_of=AS_OF,
                features={"alpha": factor_value},
                factor_run_ids=("factor-run-1",),
                input_data_refs=("duckdb://curated_market_bar",),
                warmup_complete=True,
                quality_flags=(),
                feature_ref="duckdb://feature_table?factor_run_id=factor-run-1",
                created_at="2026-07-02T00:00:00+00:00",
            )
        )
        labels.append(
            LabelValue(
                label_run_id="label-run-1",
                label_set_id="forward-v1",
                dataset_id="fixture-daily",
                symbol=symbol,
                freq="1d",
                as_of=AS_OF,
                label_id="forward_ret_1",
                label_version="1.0.0",
                value_float=label_value,
                value_string=None,
                value_kind="null" if label_value is None else "float",
                forward_bars=1,
                source_factor_run_id="factor-run-1",
                created_at="2026-07-02T00:00:00+00:00",
            )
        )
    return snapshots, labels


def _metric(computation, kind: EvaluationMetricKind, *, quantile=None):
    return next(
        metric
        for metric in computation.metrics
        if metric.metric_kind == kind and metric.quantile == quantile
    )


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        ([0.0, 1.0, 2.0, 3.0], 1.0),
        ([3.0, 2.0, 1.0, 0.0], -1.0),
    ],
)
def test_perfect_positive_and_negative_cross_sections(labels, expected):
    snapshots, label_values = _inputs([0.0, 1.0, 2.0, 3.0], labels)

    computation = evaluate_cross_sections(_request(), snapshots, label_values)

    assert _metric(computation, EvaluationMetricKind.PEARSON_IC).metric_value == pytest.approx(
        expected
    )
    assert _metric(
        computation, EvaluationMetricKind.SPEARMAN_RANK_IC
    ).metric_value == pytest.approx(expected)


def test_spearman_uses_average_ranks_for_ties():
    snapshots, labels = _inputs([1.0, 1.0, 2.0, 3.0], [1.0, 2.0, 2.0, 3.0])

    computation = evaluate_cross_sections(_request(), snapshots, labels)

    rank_ic = _metric(computation, EvaluationMetricKind.SPEARMAN_RANK_IC)
    assert rank_ic.metric_value == pytest.approx(5 / 6)


def test_quantile_ties_are_deterministic_and_long_short_uses_high_minus_low():
    snapshots, labels = _inputs([1.0] * 6, [0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    request = _request(minimum_cross_section_size=4)

    first = evaluate_cross_sections(request, list(reversed(snapshots)), labels)
    second = evaluate_cross_sections(request, snapshots, list(reversed(labels)))

    assert evaluation_content_hash(first.metrics) == evaluation_content_hash(second.metrics)
    assert _metric(
        first,
        EvaluationMetricKind.QUANTILE_RETURN,
        quantile=1,
    ).metric_value == pytest.approx(1.0)
    assert _metric(
        first,
        EvaluationMetricKind.QUANTILE_RETURN,
        quantile=2,
    ).metric_value == pytest.approx(4.0)
    assert _metric(
        first,
        EvaluationMetricKind.LONG_SHORT_RETURN,
    ).metric_value == pytest.approx(3.0)
    assert (
        _metric(
            first,
            EvaluationMetricKind.PEARSON_IC,
        ).metric_status
        == EvaluationMetricStatus.CONSTANT_INPUT
    )


def test_seeded_random_cross_section_has_small_ic():
    generator = random.Random(20260719)
    factors = [generator.uniform(-1, 1) for _ in range(200)]
    labels_raw = [generator.uniform(-1, 1) for _ in range(200)]
    snapshots, labels = _inputs(factors, labels_raw)

    computation = evaluate_cross_sections(
        _request(minimum_cross_section_size=30, quantile_count=5),
        snapshots,
        labels,
    )

    assert abs(_metric(computation, EvaluationMetricKind.PEARSON_IC).metric_value) < 0.15
    assert abs(_metric(computation, EvaluationMetricKind.SPEARMAN_RANK_IC).metric_value) < 0.15


def test_undersized_cross_section_is_auditable_and_has_no_quantiles():
    snapshots, labels = _inputs([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])

    computation = evaluate_cross_sections(_request(minimum_cross_section_size=4), snapshots, labels)

    assert computation.evaluated_cross_section_count == 0
    assert computation.skipped_cross_section_count == 1
    assert len(computation.metrics) == 2
    assert {metric.metric_status for metric in computation.metrics} == {
        EvaluationMetricStatus.INSUFFICIENT_SAMPLE
    }
    assert all(metric.metric_value is None for metric in computation.metrics)


def test_constant_label_marks_correlations_but_keeps_quantile_metrics():
    snapshots, labels = _inputs([1.0, 2.0, 3.0, 4.0], [0.5] * 4)

    computation = evaluate_cross_sections(_request(), snapshots, labels)

    correlations = [
        metric
        for metric in computation.metrics
        if metric.metric_kind
        in {EvaluationMetricKind.PEARSON_IC, EvaluationMetricKind.SPEARMAN_RANK_IC}
    ]
    assert all(
        metric.metric_status == EvaluationMetricStatus.CONSTANT_INPUT for metric in correlations
    )
    assert _metric(
        computation, EvaluationMetricKind.LONG_SHORT_RETURN
    ).metric_value == pytest.approx(0.0)
