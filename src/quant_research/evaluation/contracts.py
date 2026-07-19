from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import StrEnum

from quant_research.contracts.refs import DataRef


class EvaluationRunStatus(StrEnum):
    COMMITTED = "COMMITTED"


class EvaluationMetricKind(StrEnum):
    PEARSON_IC = "PEARSON_IC"
    SPEARMAN_RANK_IC = "SPEARMAN_RANK_IC"
    QUANTILE_RETURN = "QUANTILE_RETURN"
    LONG_SHORT_RETURN = "LONG_SHORT_RETURN"


class EvaluationMetricStatus(StrEnum):
    COMPUTED = "COMPUTED"
    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    CONSTANT_INPUT = "CONSTANT_INPUT"


class LongShortDirection(StrEnum):
    HIGH_MINUS_LOW = "HIGH_MINUS_LOW"
    LOW_MINUS_HIGH = "LOW_MINUS_HIGH"


class FactorEvaluationError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class FactorEvaluationRequest:
    evaluation_run_id: str
    feature_snapshot_ref: str
    label_ref: str
    factor_fields: tuple[str, ...]
    label_field: str
    quantile_count: int = 5
    minimum_cross_section_size: int = 30
    long_short_direction: LongShortDirection = LongShortDirection.HIGH_MINUS_LOW

    def __post_init__(self) -> None:
        if not self.evaluation_run_id.strip():
            raise ValueError("evaluation_run_id is required")
        feature_ref = DataRef.parse(self.feature_snapshot_ref)
        if feature_ref.table != "feature_snapshot":
            raise ValueError("feature_snapshot_ref must reference feature_snapshot")
        label_ref = DataRef.parse(self.label_ref)
        if label_ref.table != "label_table":
            raise ValueError("label_ref must reference label_table")
        fields = tuple(sorted(set(self.factor_fields)))
        if not fields or any(not field.strip() for field in fields):
            raise ValueError("factor_fields must contain at least one non-empty field")
        object.__setattr__(self, "factor_fields", fields)
        if not self.label_field.strip():
            raise ValueError("label_field is required")
        if self.quantile_count < 2:
            raise ValueError("quantile_count must be >= 2")
        if self.minimum_cross_section_size < 2:
            raise ValueError("minimum_cross_section_size must be >= 2")


@dataclass(frozen=True)
class FactorEvaluationMetric:
    evaluation_run_id: str
    factor_field: str
    label_field: str
    as_of: str
    metric_kind: EvaluationMetricKind
    metric_status: EvaluationMetricStatus
    metric_value: float | None
    sample_count: int
    quantile: int | None = None


@dataclass(frozen=True)
class FactorEvaluationManifest:
    evaluation_run_id: str
    feature_snapshot_ref: str
    label_ref: str
    factor_run_id: str
    label_run_id: str
    dataset_id: str
    freq: str
    factor_fields: tuple[str, ...]
    label_field: str
    quantile_count: int
    minimum_cross_section_size: int
    long_short_direction: LongShortDirection
    row_count_aligned: int
    evaluated_cross_section_count: int
    skipped_cross_section_count: int
    metric_count: int
    status: EvaluationRunStatus
    created_at: str
    code_version: str
    config_hash: str
    content_hash: str
    metric_ref: str
    rank_tie_method: str = "average"
    quantile_tie_breaker: str = "symbol"


@dataclass(frozen=True)
class FactorEvaluationResult:
    evaluation_run_id: str
    status: EvaluationRunStatus
    manifest_ref: DataRef
    metric_ref: DataRef
    row_count_aligned: int
    evaluated_cross_section_count: int
    skipped_cross_section_count: int
    metric_count: int
    reused_existing: bool = False


@dataclass(frozen=True)
class FactorEvaluationCommitResult:
    manifest: FactorEvaluationManifest
    reused_existing: bool = False


def evaluation_manifest_ref(evaluation_run_id: str) -> DataRef:
    return DataRef(
        "factor_evaluation_manifest",
        {"evaluation_run_id": evaluation_run_id},
    )


def evaluation_metric_ref(evaluation_run_id: str) -> DataRef:
    return DataRef(
        "factor_evaluation_metric",
        {"evaluation_run_id": evaluation_run_id},
    )


def evaluation_config_hash(request: FactorEvaluationRequest) -> str:
    return evaluation_hash(
        {
            "feature_snapshot_ref": canonical_data_ref(request.feature_snapshot_ref),
            "label_ref": canonical_data_ref(request.label_ref),
            "factor_fields": request.factor_fields,
            "label_field": request.label_field,
            "quantile_count": request.quantile_count,
            "minimum_cross_section_size": request.minimum_cross_section_size,
            "long_short_direction": request.long_short_direction.value,
            "rank_tie_method": "average",
            "quantile_tie_breaker": "symbol",
        }
    )


def canonical_data_ref(value: DataRef | str) -> str:
    data_ref = DataRef.parse(value) if isinstance(value, str) else value
    return DataRef(data_ref.table, dict(sorted(data_ref.filters.items()))).uri


def evaluation_content_hash(metrics: tuple[FactorEvaluationMetric, ...]) -> str:
    rows = []
    for metric in sorted(metrics, key=_metric_sort_key):
        row = asdict(metric)
        row["metric_kind"] = metric.metric_kind.value
        row["metric_status"] = metric.metric_status.value
        rows.append(row)
    return evaluation_hash(rows)


def evaluation_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _metric_sort_key(metric: FactorEvaluationMetric) -> tuple[object, ...]:
    return (
        metric.factor_field,
        metric.as_of,
        metric.metric_kind.value,
        metric.quantile or 0,
    )
