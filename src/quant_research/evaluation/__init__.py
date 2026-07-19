"""Quality-gated factor effectiveness evaluation."""

from quant_research.evaluation.analytics import (
    EvaluationComputation,
    evaluate_cross_sections,
)
from quant_research.evaluation.contracts import (
    EvaluationMetricKind,
    EvaluationMetricStatus,
    EvaluationRunStatus,
    FactorEvaluationCommitResult,
    FactorEvaluationError,
    FactorEvaluationManifest,
    FactorEvaluationMetric,
    FactorEvaluationRequest,
    FactorEvaluationResult,
    LongShortDirection,
    canonical_data_ref,
    evaluation_config_hash,
    evaluation_content_hash,
    evaluation_manifest_ref,
    evaluation_metric_ref,
)
from quant_research.evaluation.duckdb_store import LocalDuckDBEvaluationStore
from quant_research.evaluation.pipeline import FactorEvaluationPipeline

__all__ = [
    "EvaluationComputation",
    "EvaluationMetricKind",
    "EvaluationMetricStatus",
    "EvaluationRunStatus",
    "FactorEvaluationCommitResult",
    "FactorEvaluationError",
    "FactorEvaluationManifest",
    "FactorEvaluationMetric",
    "FactorEvaluationPipeline",
    "FactorEvaluationRequest",
    "FactorEvaluationResult",
    "LocalDuckDBEvaluationStore",
    "LongShortDirection",
    "canonical_data_ref",
    "evaluate_cross_sections",
    "evaluation_config_hash",
    "evaluation_content_hash",
    "evaluation_manifest_ref",
    "evaluation_metric_ref",
]
