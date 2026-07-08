from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

import polars as pl

from quant_research.factors.contracts import FactorRunConfig
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.quality import FactorQualityMetric, QualitySeverity


class CutpointSelectionMode(StrEnum):
    EVENLY_SPACED = "evenly_spaced"
    PERIOD_END = "period_end"
    EXPLICIT = "explicit"


class CompareWindowMode(StrEnum):
    TAIL_BARS = "tail_bars"
    ALL_HISTORY = "all_history"


@dataclass(frozen=True)
class PrefixProbeConfig:
    enabled: bool = True
    cutpoint_mode: CutpointSelectionMode = CutpointSelectionMode.EVENLY_SPACED
    cutpoint_count: int = 5
    explicit_cutpoints: tuple[str, ...] = ()
    period: str | None = None
    min_prefix_rows: int = 20
    compare_window_mode: CompareWindowMode = CompareWindowMode.TAIL_BARS
    compare_tail_bars: int = 20
    min_compare_rows: int = 1
    rtol: float = 1e-9
    atol: float = 1e-12
    nulls_equal: bool = True
    max_examples: int = 20


@dataclass(frozen=True)
class PrefixLeakageExample:
    cutpoint: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    factor_id: str
    output_field: str
    prefix_value: object
    full_value: object


@dataclass(frozen=True)
class PrefixProbeWarning:
    code: str
    message: str
    cutpoint: str | None = None


@dataclass(frozen=True)
class PrefixLeakageReport:
    factor_run_id: str
    feature_set_id: str
    checked_cutpoint_count: int
    compared_value_count: int
    violation_count: int
    examples: tuple[PrefixLeakageExample, ...]
    warnings: tuple[PrefixProbeWarning, ...] = ()
    cutpoints: tuple[str, ...] = ()
    cutpoint_mode: str = ""
    compare_window_mode: str = ""
    compare_tail_bars: int = 0


@dataclass(frozen=True)
class _Cutpoint:
    value: object
    as_of: str


@dataclass(frozen=True)
class _ComparisonValue:
    cutpoint: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    factor_id: str
    output_field: str
    value: object

    @property
    def key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.dataset_id,
            self.symbol,
            self.freq,
            self.as_of,
            self.factor_id,
            self.output_field,
        )


class PrefixInvarianceLeakageDetector:
    def analyze(
        self,
        *,
        input_frame: pl.LazyFrame,
        config: FactorRunConfig,
        runner: PolarsFactorRunner,
        resolved_factors: tuple[RegisteredFactor, ...],
        probe_config: PrefixProbeConfig = PrefixProbeConfig(),
    ) -> PrefixLeakageReport:
        if not probe_config.enabled:
            return self._empty_report(config, probe_config)

        input_df = input_frame.sort(["symbol", "as_of"]).collect()
        cutpoints, warnings = self._select_cutpoints(input_df, probe_config)
        if not cutpoints:
            return self._empty_report(config, probe_config, tuple(warnings))

        full_df = runner.run(input_frame, config).collect()
        checked_cutpoints: list[_Cutpoint] = []
        examples: list[PrefixLeakageExample] = []
        compared_value_count = 0
        violation_count = 0

        for cutpoint in cutpoints:
            full_values = self._comparison_values(
                full_df,
                cutpoint,
                resolved_factors,
                probe_config,
            )
            if len(full_values) < probe_config.min_compare_rows:
                warnings.append(
                    PrefixProbeWarning(
                        code="insufficient_compare_rows",
                        message=(
                            f"cutpoint has {len(full_values)} comparable values, "
                            f"below min_compare_rows={probe_config.min_compare_rows}"
                        ),
                        cutpoint=cutpoint.as_of,
                    )
                )
                continue

            prefix_input = input_frame.filter(pl.col("as_of") <= cutpoint.value)
            prefix_df = runner.run(prefix_input, config).collect()
            prefix_values = self._comparison_values(
                prefix_df,
                cutpoint,
                resolved_factors,
                probe_config,
            )
            checked_cutpoints.append(cutpoint)
            prefix_by_key = {value.key: value for value in prefix_values}
            for full_value in full_values:
                compared_value_count += 1
                prefix_value = prefix_by_key.get(full_value.key)
                if prefix_value is None or not self._values_equal(
                    prefix_value.value,
                    full_value.value,
                    probe_config,
                ):
                    violation_count += 1
                    if len(examples) < probe_config.max_examples:
                        examples.append(
                            PrefixLeakageExample(
                                cutpoint=cutpoint.as_of,
                                dataset_id=full_value.dataset_id,
                                symbol=full_value.symbol,
                                freq=full_value.freq,
                                as_of=full_value.as_of,
                                factor_id=full_value.factor_id,
                                output_field=full_value.output_field,
                                prefix_value=prefix_value.value if prefix_value else None,
                                full_value=full_value.value,
                            )
                        )

        return PrefixLeakageReport(
            factor_run_id=config.factor_run_id,
            feature_set_id=config.feature_set_id,
            checked_cutpoint_count=len(checked_cutpoints),
            compared_value_count=compared_value_count,
            violation_count=violation_count,
            examples=tuple(examples),
            warnings=tuple(warnings),
            cutpoints=tuple(cutpoint.as_of for cutpoint in checked_cutpoints),
            cutpoint_mode=probe_config.cutpoint_mode.value,
            compare_window_mode=probe_config.compare_window_mode.value,
            compare_tail_bars=probe_config.compare_tail_bars,
        )

    def _empty_report(
        self,
        config: FactorRunConfig,
        probe_config: PrefixProbeConfig | None = None,
        warnings: tuple[PrefixProbeWarning, ...] = (),
    ) -> PrefixLeakageReport:
        return PrefixLeakageReport(
            factor_run_id=config.factor_run_id,
            feature_set_id=config.feature_set_id,
            checked_cutpoint_count=0,
            compared_value_count=0,
            violation_count=0,
            examples=(),
            warnings=warnings,
            cutpoint_mode=probe_config.cutpoint_mode.value if probe_config else "",
            compare_window_mode=probe_config.compare_window_mode.value if probe_config else "",
            compare_tail_bars=probe_config.compare_tail_bars if probe_config else 0,
        )

    def _select_cutpoints(
        self,
        input_df: pl.DataFrame,
        probe_config: PrefixProbeConfig,
    ) -> tuple[list[_Cutpoint], list[PrefixProbeWarning]]:
        warnings: list[PrefixProbeWarning] = []
        rows = input_df.select("as_of").unique().sort("as_of").to_dicts()
        as_of_values = [row["as_of"] for row in rows]
        if len(as_of_values) < 2:
            return [], warnings
        value_by_as_of = {self._format_as_of(value): value for value in as_of_values}
        final_as_of = self._format_as_of(as_of_values[-1])

        if probe_config.cutpoint_mode == CutpointSelectionMode.EXPLICIT:
            selected: list[_Cutpoint] = []
            for as_of in probe_config.explicit_cutpoints:
                value = value_by_as_of.get(as_of)
                if value is None:
                    warnings.append(
                        PrefixProbeWarning(
                            code="missing_explicit_cutpoint",
                            message="explicit cutpoint does not exist in input as_of values",
                            cutpoint=as_of,
                        )
                    )
                    continue
                if as_of == final_as_of:
                    warnings.append(
                        PrefixProbeWarning(
                            code="final_explicit_cutpoint",
                            message="final as_of cannot be used as a prefix cutpoint",
                            cutpoint=as_of,
                        )
                    )
                    continue
                prefix_rows = input_df.filter(pl.col("as_of") <= value).height
                if prefix_rows >= probe_config.min_prefix_rows:
                    selected.append(_Cutpoint(value=value, as_of=as_of))
                else:
                    warnings.append(
                        PrefixProbeWarning(
                            code="insufficient_prefix_rows",
                            message=(
                                f"cutpoint has {prefix_rows} prefix rows, "
                                f"below min_prefix_rows={probe_config.min_prefix_rows}"
                            ),
                            cutpoint=as_of,
                        )
                    )
            return selected, warnings

        if probe_config.cutpoint_mode == CutpointSelectionMode.PERIOD_END:
            period = probe_config.period or "month"
            by_period: dict[str, _Cutpoint] = {}
            for value in as_of_values[:-1]:
                prefix_rows = input_df.filter(pl.col("as_of") <= value).height
                if prefix_rows >= probe_config.min_prefix_rows:
                    by_period[self._period_key(value, period)] = _Cutpoint(
                        value=value,
                        as_of=self._format_as_of(value),
                    )
            return list(by_period.values()), warnings

        usable = [
            _Cutpoint(value=value, as_of=self._format_as_of(value))
            for value in as_of_values[:-1]
            if input_df.filter(pl.col("as_of") <= value).height >= probe_config.min_prefix_rows
        ]
        if len(usable) <= probe_config.cutpoint_count:
            return usable, warnings
        if probe_config.cutpoint_count <= 1:
            return [usable[-1]], warnings
        step = (len(usable) - 1) / (probe_config.cutpoint_count - 1)
        return [
            usable[round(index * step)] for index in range(probe_config.cutpoint_count)
        ], warnings

    def _comparison_values(
        self,
        frame: pl.DataFrame,
        cutpoint: _Cutpoint,
        resolved_factors: tuple[RegisteredFactor, ...],
        probe_config: PrefixProbeConfig,
    ) -> list[_ComparisonValue]:
        scoped = frame.filter(pl.col("as_of") <= cutpoint.value).sort(["symbol", "as_of"])
        if probe_config.compare_window_mode == CompareWindowMode.TAIL_BARS:
            tail_bars = probe_config.compare_tail_bars
            if tail_bars > 0:
                scoped = scoped.group_by(
                    ["dataset_id", "symbol", "freq"], maintain_order=True
                ).tail(tail_bars)

        rows = scoped.to_dicts()
        values: list[_ComparisonValue] = []
        for row in rows:
            for registered in resolved_factors:
                spec = registered.spec
                for output_field in spec.output_fields:
                    values.append(
                        _ComparisonValue(
                            cutpoint=cutpoint.as_of,
                            dataset_id=str(row["dataset_id"]),
                            symbol=str(row["symbol"]),
                            freq=str(row["freq"]),
                            as_of=self._format_as_of(row["as_of"]),
                            factor_id=spec.factor_id,
                            output_field=output_field,
                            value=self._normalize_value(row.get(output_field)),
                        )
                    )
        return values

    def _values_equal(
        self,
        left: object,
        right: object,
        probe_config: PrefixProbeConfig,
    ) -> bool:
        left = self._normalize_value(left)
        right = self._normalize_value(right)
        if left is None or right is None:
            return probe_config.nulls_equal and left is None and right is None
        if isinstance(left, float) or isinstance(right, float):
            left_float = float(left)
            right_float = float(right)
            if math.isnan(left_float) or math.isnan(right_float):
                return (
                    probe_config.nulls_equal
                    and math.isnan(left_float)
                    and math.isnan(right_float)
                )
            return math.isclose(
                left_float,
                right_float,
                rel_tol=probe_config.rtol,
                abs_tol=probe_config.atol,
            )
        return left == right

    def _normalize_value(self, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    def _format_as_of(self, value: object) -> str:
        if isinstance(value, datetime | date):
            return value.isoformat()
        if isinstance(value, str):
            return value
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _period_key(self, value: object, period: str) -> str:
        as_datetime = self._as_datetime(value)
        if period == "week":
            year, week, _ = as_datetime.isocalendar()
            return f"{year}-W{week:02d}"
        if period == "month":
            return f"{as_datetime.year}-{as_datetime.month:02d}"
        if period == "quarter":
            quarter = (as_datetime.month - 1) // 3 + 1
            return f"{as_datetime.year}-Q{quarter}"
        raise ValueError(f"unsupported prefix probe period: {period}")

    def _as_datetime(self, value: object) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        raise TypeError(f"unsupported as_of value for period selection: {value!r}")


def prefix_report_to_quality_metrics(
    report: PrefixLeakageReport,
    *,
    factor_id: str = "__prefix_probe__",
    output_field: str = "__all__",
    created_at: str | None = None,
) -> tuple[FactorQualityMetric, ...]:
    created_at = created_at or datetime.now(UTC).isoformat()
    changed_ratio = (
        report.violation_count / report.compared_value_count
        if report.compared_value_count
        else 0.0
    )
    examples = [
        {
            "cutpoint": example.cutpoint,
            "dataset_id": example.dataset_id,
            "symbol": example.symbol,
            "freq": example.freq,
            "as_of": example.as_of,
            "factor_id": example.factor_id,
            "output_field": example.output_field,
            "prefix_value": example.prefix_value,
            "full_value": example.full_value,
        }
        for example in report.examples
    ]
    warnings = [
        {
            "code": warning.code,
            "message": warning.message,
            "cutpoint": warning.cutpoint,
        }
        for warning in report.warnings
    ]
    return (
        _quality_metric(
            report,
            factor_id,
            output_field,
            "prefix_invariance_violation_count",
            report.violation_count,
            {
                "check_level": "prefix_recompute",
                "as_of_semantics": "factor_value_timestamp",
                "cutpoint_mode": report.cutpoint_mode,
                "compare_window_mode": report.compare_window_mode,
                "compare_tail_bars": report.compare_tail_bars,
                "cutpoints": list(report.cutpoints),
                "examples": examples,
            },
            created_at,
            QualitySeverity.ERROR if report.violation_count else QualitySeverity.INFO,
        ),
        _quality_metric(
            report,
            factor_id,
            output_field,
            "prefix_probe_cutpoint_count",
            report.checked_cutpoint_count,
            {},
            created_at,
        ),
        _quality_metric(
            report,
            factor_id,
            output_field,
            "prefix_probe_compared_value_count",
            report.compared_value_count,
            {},
            created_at,
        ),
        _quality_metric(
            report,
            factor_id,
            output_field,
            "prefix_probe_warning_count",
            len(report.warnings),
            {"warnings": warnings},
            created_at,
            QualitySeverity.WARNING if report.warnings else QualitySeverity.INFO,
        ),
        _quality_metric(
            report,
            factor_id,
            output_field,
            "prefix_probe_changed_ratio",
            changed_ratio,
            {
                "violation_count": report.violation_count,
                "compared_value_count": report.compared_value_count,
            },
            created_at,
            QualitySeverity.ERROR if changed_ratio else QualitySeverity.INFO,
        ),
    )


def _quality_metric(
    report: PrefixLeakageReport,
    factor_id: str,
    output_field: str,
    metric_name: str,
    metric_value: float,
    metric_json: dict[str, Any],
    created_at: str,
    severity: QualitySeverity = QualitySeverity.INFO,
) -> FactorQualityMetric:
    return FactorQualityMetric(
        factor_run_id=report.factor_run_id,
        feature_set_id=report.feature_set_id,
        factor_id=factor_id,
        output_field=output_field,
        metric_name=metric_name,
        metric_value=float(metric_value),
        metric_json=metric_json,
        severity=severity,
        created_at=created_at,
    )
