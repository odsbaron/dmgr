from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureValue


class QualitySeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class QualityStatus(StrEnum):
    NOT_RUN = "NOT_RUN"
    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"


@dataclass(frozen=True)
class FactorQualityMetric:
    factor_run_id: str
    feature_set_id: str
    factor_id: str
    output_field: str
    metric_name: str
    metric_value: float
    metric_json: dict[str, Any]
    severity: QualitySeverity
    created_at: str


@dataclass(frozen=True)
class FactorQualityReport:
    factor_run_id: str
    feature_set_id: str
    status: QualityStatus
    metrics: tuple[FactorQualityMetric, ...]

    @property
    def summary(self) -> dict[str, Any]:
        by_severity = Counter(metric.severity.value for metric in self.metrics)
        return {
            "status": self.status.value,
            "metric_count": len(self.metrics),
            "severity_counts": dict(sorted(by_severity.items())),
        }


class FactorQualityAnalyzer:
    def analyze(
        self,
        values: list[FeatureValue],
        resolved_factors: tuple[RegisteredFactor, ...],
    ) -> FactorQualityReport:
        if not values:
            return FactorQualityReport(
                factor_run_id="",
                feature_set_id="",
                status=QualityStatus.FAILED,
                metrics=(),
            )

        created_at = datetime.now(UTC).isoformat()
        metrics: list[FactorQualityMetric] = []
        for registered in resolved_factors:
            for output_field in registered.spec.output_fields:
                scoped_values = [
                    value
                    for value in values
                    if value.factor_id == registered.spec.factor_id
                    and value.output_field == output_field
                ]
                metrics.extend(
                    self._metrics_for_output(registered, output_field, scoped_values, created_at)
                )

        status = self._status(metrics)
        return FactorQualityReport(
            factor_run_id=values[0].factor_run_id,
            feature_set_id=values[0].feature_set_id,
            status=status,
            metrics=tuple(metrics),
        )

    def _metrics_for_output(
        self,
        registered: RegisteredFactor,
        output_field: str,
        values: list[FeatureValue],
        created_at: str,
    ) -> list[FactorQualityMetric]:
        spec = registered.spec
        row_count = len(values)
        null_count = sum(1 for value in values if value.value_kind == "null")
        null_ratio = null_count / row_count if row_count else 1.0
        max_null_ratio = float(spec.quality_rules.get("max_null_ratio", 1.0))
        warmup_incomplete_count = sum(1 for value in values if not value.warmup_complete)
        duplicate_count = self._duplicate_count(values)
        forward_bars = int(spec.quality_rules.get("forward_bars", 0) or 0)
        uses_future_data = bool(spec.quality_rules.get("uses_future_data", False))
        causal = spec.quality_rules.get("causal", True)
        is_forward_calculation = forward_bars > 0 or uses_future_data or causal is False
        future_leakage_count = row_count if is_forward_calculation else 0

        return [
            self._metric(spec.factor_id, output_field, values, "row_count", row_count, {}, created_at),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "null_ratio",
                null_ratio,
                {"null_count": null_count, "max_null_ratio": max_null_ratio},
                created_at,
                QualitySeverity.ERROR if null_ratio > max_null_ratio else QualitySeverity.INFO,
            ),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "warmup_incomplete_count",
                warmup_incomplete_count,
                {},
                created_at,
            ),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "duplicate_key_count",
                duplicate_count,
                {},
                created_at,
                QualitySeverity.ERROR if duplicate_count else QualitySeverity.INFO,
            ),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "future_leakage_count",
                future_leakage_count,
                {
                    "check_level": "forward_metadata",
                    "forward_bars": forward_bars,
                    "uses_future_data": uses_future_data,
                    "causal": causal,
                },
                created_at,
                QualitySeverity.ERROR if future_leakage_count else QualitySeverity.INFO,
            ),
        ]

    def _metric(
        self,
        factor_id: str,
        output_field: str,
        values: list[FeatureValue],
        metric_name: str,
        metric_value: float,
        metric_json: dict[str, Any],
        created_at: str,
        severity: QualitySeverity = QualitySeverity.INFO,
    ) -> FactorQualityMetric:
        return FactorQualityMetric(
            factor_run_id=values[0].factor_run_id if values else "",
            feature_set_id=values[0].feature_set_id if values else "",
            factor_id=factor_id,
            output_field=output_field,
            metric_name=metric_name,
            metric_value=float(metric_value),
            metric_json=metric_json,
            severity=severity,
            created_at=created_at,
        )

    def _duplicate_count(self, values: list[FeatureValue]) -> int:
        counts = Counter(
            (
                value.feature_set_id,
                value.dataset_id,
                value.symbol,
                value.freq,
                value.as_of,
                value.factor_id,
                value.factor_version,
                value.output_field,
            )
            for value in values
        )
        return sum(count - 1 for count in counts.values() if count > 1)

    def _status(self, metrics: list[FactorQualityMetric]) -> QualityStatus:
        if any(metric.severity == QualitySeverity.ERROR for metric in metrics):
            return QualityStatus.FAILED
        if any(metric.severity == QualitySeverity.WARNING for metric in metrics):
            return QualityStatus.WARNING
        return QualityStatus.PASSED
