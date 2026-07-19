from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from numbers import Real

import polars as pl

from quant_research.evaluation.contracts import (
    EvaluationMetricKind,
    EvaluationMetricStatus,
    FactorEvaluationError,
    FactorEvaluationMetric,
    FactorEvaluationRequest,
    LongShortDirection,
)
from quant_research.features.contracts import FeatureSnapshot
from quant_research.labels.contracts import LabelValue


@dataclass(frozen=True)
class EvaluationComputation:
    metrics: tuple[FactorEvaluationMetric, ...]
    row_count_aligned: int
    evaluated_cross_section_count: int
    skipped_cross_section_count: int


def evaluate_cross_sections(
    request: FactorEvaluationRequest,
    snapshots: list[FeatureSnapshot],
    labels: list[LabelValue],
) -> EvaluationComputation:
    label_values = _selected_labels(labels, request.label_field)
    snapshot_values = _unique_snapshots(snapshots)
    _assert_requested_fields(snapshot_values.values(), request.factor_fields)

    aligned_keys = sorted(set(snapshot_values) & set(label_values))
    if not aligned_keys:
        raise FactorEvaluationError(
            "NO_ALIGNED_ROWS",
            "feature snapshots and labels have no matching point-in-time rows",
        )

    as_of_values = sorted({key[3] for key in aligned_keys})
    grouped: dict[tuple[str, str], list[tuple[str, float, float]]] = defaultdict(list)
    for key in aligned_keys:
        snapshot = snapshot_values[key]
        label_value = _finite_float(label_values[key].value)
        if label_value is None:
            continue
        for factor_field in request.factor_fields:
            factor_value = _finite_float(snapshot.features.get(factor_field))
            if factor_value is not None:
                grouped[(factor_field, key[3])].append((key[1], factor_value, label_value))

    metrics: list[FactorEvaluationMetric] = []
    evaluated = 0
    skipped = 0
    for factor_field in request.factor_fields:
        for as_of in as_of_values:
            rows = sorted(grouped[(factor_field, as_of)], key=lambda row: row[0])
            cross_section_metrics, was_skipped = _evaluate_one_cross_section(
                request,
                factor_field,
                as_of,
                rows,
            )
            metrics.extend(cross_section_metrics)
            if was_skipped:
                skipped += 1
            else:
                evaluated += 1

    return EvaluationComputation(
        metrics=tuple(metrics),
        row_count_aligned=len(aligned_keys),
        evaluated_cross_section_count=evaluated,
        skipped_cross_section_count=skipped,
    )


def _evaluate_one_cross_section(
    request: FactorEvaluationRequest,
    factor_field: str,
    as_of: str,
    rows: list[tuple[str, float, float]],
) -> tuple[list[FactorEvaluationMetric], bool]:
    sample_count = len(rows)
    if sample_count < request.minimum_cross_section_size:
        return (
            [
                _metric(
                    request,
                    factor_field,
                    as_of,
                    kind,
                    EvaluationMetricStatus.INSUFFICIENT_SAMPLE,
                    None,
                    sample_count,
                )
                for kind in (
                    EvaluationMetricKind.PEARSON_IC,
                    EvaluationMetricKind.SPEARMAN_RANK_IC,
                )
            ],
            True,
        )

    frame = pl.DataFrame(
        {
            "factor": [row[1] for row in rows],
            "label": [row[2] for row in rows],
        }
    )
    factor_constant = frame["factor"].n_unique() < 2
    label_constant = frame["label"].n_unique() < 2
    pearson_status = (
        EvaluationMetricStatus.CONSTANT_INPUT
        if factor_constant or label_constant
        else EvaluationMetricStatus.COMPUTED
    )
    spearman_status = pearson_status
    pearson = None if pearson_status != EvaluationMetricStatus.COMPUTED else _pearson(frame)
    spearman = None if spearman_status != EvaluationMetricStatus.COMPUTED else _spearman(frame)
    metrics = [
        _metric(
            request,
            factor_field,
            as_of,
            EvaluationMetricKind.PEARSON_IC,
            pearson_status,
            pearson,
            sample_count,
        ),
        _metric(
            request,
            factor_field,
            as_of,
            EvaluationMetricKind.SPEARMAN_RANK_IC,
            spearman_status,
            spearman,
            sample_count,
        ),
    ]
    metrics.extend(_quantile_metrics(request, factor_field, as_of, rows))
    return metrics, False


def _pearson(frame: pl.DataFrame) -> float:
    value = frame.select(pl.corr("factor", "label")).item()
    return float(value)


def _spearman(frame: pl.DataFrame) -> float:
    value = (
        frame.with_columns(
            pl.col("factor").rank(method="average").alias("factor_rank"),
            pl.col("label").rank(method="average").alias("label_rank"),
        )
        .select(pl.corr("factor_rank", "label_rank"))
        .item()
    )
    return float(value)


def _quantile_metrics(
    request: FactorEvaluationRequest,
    factor_field: str,
    as_of: str,
    rows: list[tuple[str, float, float]],
) -> list[FactorEvaluationMetric]:
    ordered = sorted(rows, key=lambda row: (row[1], row[0]))
    buckets: dict[int, list[float]] = defaultdict(list)
    sample_count = len(ordered)
    for position, (_, _, label_value) in enumerate(ordered):
        quantile = (position * request.quantile_count) // sample_count + 1
        buckets[quantile].append(label_value)

    means = {quantile: sum(values) / len(values) for quantile, values in sorted(buckets.items())}
    metrics = [
        _metric(
            request,
            factor_field,
            as_of,
            EvaluationMetricKind.QUANTILE_RETURN,
            EvaluationMetricStatus.COMPUTED,
            mean,
            len(buckets[quantile]),
            quantile=quantile,
        )
        for quantile, mean in means.items()
    ]
    low = means[min(means)]
    high = means[max(means)]
    long_short = high - low
    if request.long_short_direction == LongShortDirection.LOW_MINUS_HIGH:
        long_short = -long_short
    metrics.append(
        _metric(
            request,
            factor_field,
            as_of,
            EvaluationMetricKind.LONG_SHORT_RETURN,
            EvaluationMetricStatus.COMPUTED,
            long_short,
            sample_count,
        )
    )
    return metrics


def _metric(
    request: FactorEvaluationRequest,
    factor_field: str,
    as_of: str,
    kind: EvaluationMetricKind,
    status: EvaluationMetricStatus,
    value: float | None,
    sample_count: int,
    *,
    quantile: int | None = None,
) -> FactorEvaluationMetric:
    return FactorEvaluationMetric(
        evaluation_run_id=request.evaluation_run_id,
        factor_field=factor_field,
        label_field=request.label_field,
        as_of=as_of,
        metric_kind=kind,
        metric_status=status,
        metric_value=value,
        sample_count=sample_count,
        quantile=quantile,
    )


def _selected_labels(
    labels: list[LabelValue],
    label_field: str,
) -> dict[tuple[str, str, str, str], LabelValue]:
    selected = [label for label in labels if label.label_id == label_field]
    if not selected:
        raise FactorEvaluationError(
            "LABEL_FIELD_NOT_FOUND",
            f"label field is absent from label ref: {label_field}",
        )
    values: dict[tuple[str, str, str, str], LabelValue] = {}
    for label in selected:
        key = (label.dataset_id, label.symbol, label.freq, label.as_of)
        if key in values:
            raise FactorEvaluationError(
                "DUPLICATE_LABEL_KEY",
                f"duplicate label key for {label.symbol} at {label.as_of}",
            )
        values[key] = label
    return values


def _unique_snapshots(
    snapshots: list[FeatureSnapshot],
) -> dict[tuple[str, str, str, str], FeatureSnapshot]:
    values: dict[tuple[str, str, str, str], FeatureSnapshot] = {}
    for snapshot in snapshots:
        key = (snapshot.dataset_id, snapshot.symbol, snapshot.freq, snapshot.as_of)
        if key in values:
            raise FactorEvaluationError(
                "DUPLICATE_FEATURE_KEY",
                f"duplicate feature key for {snapshot.symbol} at {snapshot.as_of}",
            )
        values[key] = snapshot
    if not values:
        raise FactorEvaluationError("EMPTY_FEATURE_INPUT", "feature ref contains no snapshots")
    return values


def _assert_requested_fields(
    snapshots,
    factor_fields: tuple[str, ...],
) -> None:
    available = {field for snapshot in snapshots for field in snapshot.features}
    missing = sorted(set(factor_fields) - available)
    if missing:
        raise FactorEvaluationError(
            "FACTOR_FIELD_NOT_FOUND",
            f"factor fields are absent from feature ref: {', '.join(missing)}",
        )


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    converted = float(value)
    return converted if math.isfinite(converted) else None
