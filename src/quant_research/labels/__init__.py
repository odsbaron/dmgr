"""Training label contracts and storage adapters."""

from quant_research.labels.contracts import (
    LabelCommitRequest,
    LabelRunManifest,
    LabelSourceKind,
    LabelValue,
)
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.gates import LabelConsumptionBlocked, LabelQualityGate
from quant_research.labels.generation import (
    ForwardReturnLabelConfig,
    feature_values_to_label_request,
    forward_return_labels_from_bars,
)
from quant_research.labels.pipeline import (
    LabelPipeline,
    LabelRunRequest,
    LabelRunResult,
    LabelRunStatus,
)
from quant_research.labels.quality import LabelQualityAnalyzer, LabelQualityMetric, LabelQualityReport

__all__ = [
    "ForwardReturnLabelConfig",
    "LabelCommitRequest",
    "LabelConsumptionBlocked",
    "LabelPipeline",
    "LabelQualityGate",
    "LabelQualityAnalyzer",
    "LabelQualityMetric",
    "LabelQualityReport",
    "LabelRunManifest",
    "LabelSourceKind",
    "LabelRunRequest",
    "LabelRunResult",
    "LabelRunStatus",
    "LabelValue",
    "LocalDuckDBLabelStore",
    "feature_values_to_label_request",
    "forward_return_labels_from_bars",
]
