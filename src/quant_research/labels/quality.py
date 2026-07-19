from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from quant_research.features.quality import QualitySeverity, QualityStatus
from quant_research.labels.contracts import LabelValue


@dataclass(frozen=True)
class LabelQualityMetric:
    label_run_id: str
    label_set_id: str
    label_id: str
    metric_name: str
    metric_value: float
    metric_json: dict[str, Any]
    severity: QualitySeverity
    created_at: str


@dataclass(frozen=True)
class LabelQualityReport:
    label_run_id: str
    label_set_id: str
    status: QualityStatus
    metrics: tuple[LabelQualityMetric, ...]

    @property
    def summary(self) -> dict[str, Any]:
        by_severity = Counter(metric.severity.value for metric in self.metrics)
        return {
            "status": self.status.value,
            "metric_count": len(self.metrics),
            "severity_counts": dict(sorted(by_severity.items())),
        }


class LabelQualityAnalyzer:
    def __init__(self, *, max_null_ratio: float = 1.0):
        self.max_null_ratio = max_null_ratio

    def analyze(self, labels: tuple[LabelValue, ...]) -> LabelQualityReport:
        if not labels:
            return LabelQualityReport(
                label_run_id="",
                label_set_id="",
                status=QualityStatus.FAILED,
                metrics=(),
            )

        created_at = datetime.now(UTC).isoformat()
        by_label_id: dict[str, list[LabelValue]] = defaultdict(list)
        for label in labels:
            by_label_id[label.label_id].append(label)

        metrics: list[LabelQualityMetric] = []
        for label_id in sorted(by_label_id):
            metrics.extend(self._metrics_for_label(label_id, by_label_id[label_id], created_at))

        status = self._status(metrics)
        return LabelQualityReport(
            label_run_id=labels[0].label_run_id,
            label_set_id=labels[0].label_set_id,
            status=status,
            metrics=tuple(metrics),
        )

    def _metrics_for_label(
        self,
        label_id: str,
        labels: list[LabelValue],
        created_at: str,
    ) -> list[LabelQualityMetric]:
        row_count = len(labels)
        null_count = sum(1 for label in labels if label.value_kind == "null")
        null_ratio = null_count / row_count if row_count else 1.0
        as_of_values = sorted(label.as_of for label in labels)
        duplicate_count = self._duplicate_count(labels)
        return [
            self._metric(label_id, labels, "row_count", row_count, {}, created_at),
            self._metric(
                label_id,
                labels,
                "null_ratio",
                null_ratio,
                {"null_count": null_count, "max_null_ratio": self.max_null_ratio},
                created_at,
                QualitySeverity.ERROR
                if null_ratio > self.max_null_ratio
                else QualitySeverity.INFO,
            ),
            self._metric(
                label_id,
                labels,
                "symbol_count",
                len({label.symbol for label in labels}),
                {},
                created_at,
            ),
            self._metric(
                label_id,
                labels,
                "as_of_min",
                0,
                {"value": as_of_values[0]},
                created_at,
            ),
            self._metric(
                label_id,
                labels,
                "as_of_max",
                0,
                {"value": as_of_values[-1]},
                created_at,
            ),
            self._metric(
                label_id,
                labels,
                "duplicate_key_count",
                duplicate_count,
                {},
                created_at,
                QualitySeverity.ERROR if duplicate_count else QualitySeverity.INFO,
            ),
        ]

    def _metric(
        self,
        label_id: str,
        labels: list[LabelValue],
        metric_name: str,
        metric_value: float,
        metric_json: dict[str, Any],
        created_at: str,
        severity: QualitySeverity = QualitySeverity.INFO,
    ) -> LabelQualityMetric:
        return LabelQualityMetric(
            label_run_id=labels[0].label_run_id if labels else "",
            label_set_id=labels[0].label_set_id if labels else "",
            label_id=label_id,
            metric_name=metric_name,
            metric_value=float(metric_value),
            metric_json=metric_json,
            severity=severity,
            created_at=created_at,
        )

    def _duplicate_count(self, labels: list[LabelValue]) -> int:
        counts = Counter(
            (
                label.label_set_id,
                label.dataset_id,
                label.symbol,
                label.freq,
                label.as_of,
                label.label_id,
                label.label_version,
                label.source_factor_run_id,
            )
            for label in labels
        )
        return sum(count - 1 for count in counts.values() if count > 1)

    def _status(self, metrics: list[LabelQualityMetric]) -> QualityStatus:
        if any(metric.severity == QualitySeverity.ERROR for metric in metrics):
            return QualityStatus.FAILED
        if any(metric.severity == QualitySeverity.WARNING for metric in metrics):
            return QualityStatus.WARNING
        return QualityStatus.PASSED
