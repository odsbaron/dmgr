from __future__ import annotations

from dataclasses import dataclass, field, replace

from quant_research.factors.contracts import FactorRunConfig
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry
from quant_research.features.contracts import (
    FeatureCommitRequest,
    FeatureCommitResult,
    FeatureRunStatus,
)
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.leakage import (
    PrefixInvarianceLeakageDetector,
    prefix_report_to_quality_metrics,
)
from quant_research.features.quality import (
    FactorQualityMetric,
    FactorQualityReport,
    FactorQualityAnalyzer,
    QualitySeverity,
    QualityStatus,
)
from quant_research.contracts.bar import BarRecord
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.pipeline.bar_frame import bars_to_factor_frame
from quant_research.pipeline.contracts import (
    PipelineInputRefError,
    ResearchRunRequest,
    ResearchRunResult,
    ResearchRunStatus,
    parse_pipeline_input_ref,
)


@dataclass
class ResearchPipeline:
    data_store: LocalDuckDBStore
    factor_registry: FactorRegistry
    factor_runner: PolarsFactorRunner
    feature_store: LocalDuckDBFeatureStore
    quality_analyzer: FactorQualityAnalyzer
    leakage_detector: PrefixInvarianceLeakageDetector = field(
        default_factory=PrefixInvarianceLeakageDetector
    )

    def run(self, request: ResearchRunRequest) -> ResearchRunResult:
        try:
            input_slice = parse_pipeline_input_ref(request)
        except PipelineInputRefError as exc:
            return self._failed_result(
                request,
                error_step="parse_input_ref",
                error_code="INVALID_INPUT_DATA_REF",
                error_message=str(exc),
            )

        try:
            bars = self.data_store.read_bars(input_slice.data_ref)
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="read_bars",
                error_code="READ_BARS_FAILED",
                error_message=str(exc),
            )

        try:
            filtered_bars = self._filter_bars(bars, request)
            factor_frame = bars_to_factor_frame(filtered_bars)
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="build_factor_frame",
                error_code="BUILD_FACTOR_FRAME_FAILED",
                error_message=str(exc),
                row_count_input=0,
            )

        try:
            resolved_factors = tuple(
                self.factor_registry.resolve_many(request.factor_ids, freq=input_slice.freq)
            )
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="resolve_factors",
                error_code="RESOLVE_FACTORS_FAILED",
                error_message=str(exc),
                row_count_input=len(filtered_bars),
            )

        config = FactorRunConfig(
            factor_run_id=request.factor_run_id,
            feature_set_id=request.feature_set_id,
            input_data_ref=input_slice.data_ref.uri,
            factor_ids=request.factor_ids,
            freq=input_slice.freq,
            dataset_id=input_slice.dataset_id,
            as_of_start=request.as_of_start,
            as_of_end=request.as_of_end,
            symbols=request.symbols,
            engine=request.engine,
            execution_mode=request.execution_mode,
            seed=request.seed,
        )

        try:
            factor_result_frame = self.factor_runner.run(factor_frame, config)
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="compute_factors",
                error_code="COMPUTE_FACTORS_FAILED",
                error_message=str(exc),
                row_count_input=len(filtered_bars),
            )

        commit = self.feature_store.commit_run(
            FeatureCommitRequest(
                config=config,
                factor_frame=factor_result_frame,
                resolved_factors=resolved_factors,
                input_row_count=len(filtered_bars),
                allow_failed_overwrite=request.allow_failed_overwrite,
            )
        )
        if commit.status == FeatureRunStatus.FAILED:
            return self._failed_commit_result(request, commit, len(filtered_bars))

        values = self.feature_store.read_feature_table(commit.feature_table_ref)
        base_report = self.quality_analyzer.analyze(values, resolved_factors)
        extra_metrics: tuple[FactorQualityMetric, ...] = ()
        if request.prefix_probe_config is not None:
            prefix_report = self.leakage_detector.analyze(
                input_frame=factor_frame,
                config=config,
                runner=self.factor_runner,
                resolved_factors=resolved_factors,
                probe_config=request.prefix_probe_config,
            )
            extra_metrics = prefix_report_to_quality_metrics(prefix_report)

        merged_report = merge_quality_reports(base_report, extra_metrics)
        self.feature_store.commit_quality_report(merged_report)
        status = (
            ResearchRunStatus.COMMITTED
            if merged_report.status == QualityStatus.PASSED
            else ResearchRunStatus.QUALITY_FAILED
        )
        return ResearchRunResult.from_quality_status(
            factor_run_id=request.factor_run_id,
            status=status,
            feature_table_ref=commit.feature_table_ref,
            snapshot_ref=commit.snapshot_ref,
            manifest_ref=commit.manifest_ref,
            quality_status=merged_report.status,
            quality_summary=merged_report.summary,
            row_count_input=len(filtered_bars),
            row_count_feature=commit.row_count_feature,
            row_count_snapshot=commit.row_count_snapshot,
            metric_count=len(merged_report.metrics),
        )

    def _filter_bars(
        self,
        bars: list[BarRecord],
        request: ResearchRunRequest,
    ) -> list[BarRecord]:
        filtered = bars
        if request.symbols is not None:
            symbols = set(request.symbols)
            filtered = [bar for bar in filtered if bar.symbol in symbols]
        if request.as_of_start is not None:
            filtered = [bar for bar in filtered if bar.bar_end_time >= request.as_of_start]
        if request.as_of_end is not None:
            filtered = [bar for bar in filtered if bar.bar_end_time <= request.as_of_end]
        return filtered

    def _failed_commit_result(
        self,
        request: ResearchRunRequest,
        commit: FeatureCommitResult,
        row_count_input: int,
    ) -> ResearchRunResult:
        return ResearchRunResult.from_quality_status(
            factor_run_id=request.factor_run_id,
            status=ResearchRunStatus.FAILED,
            feature_table_ref=commit.feature_table_ref,
            snapshot_ref=commit.snapshot_ref,
            manifest_ref=commit.manifest_ref,
            quality_status=QualityStatus.NOT_RUN,
            quality_summary={},
            row_count_input=row_count_input,
            row_count_feature=commit.row_count_feature,
            row_count_snapshot=commit.row_count_snapshot,
            metric_count=0,
            error_step="commit_features",
            error_code=commit.error_code,
            error_message=commit.error_message,
        )

    def _failed_result(
        self,
        request: ResearchRunRequest,
        *,
        error_step: str,
        error_code: str,
        error_message: str,
        row_count_input: int = 0,
    ) -> ResearchRunResult:
        return ResearchRunResult.from_quality_status(
            factor_run_id=request.factor_run_id,
            status=ResearchRunStatus.FAILED,
            feature_table_ref=None,
            snapshot_ref=None,
            manifest_ref=None,
            quality_status=QualityStatus.NOT_RUN,
            quality_summary={},
            row_count_input=row_count_input,
            row_count_feature=0,
            row_count_snapshot=0,
            metric_count=0,
            error_step=error_step,
            error_code=error_code,
            error_message=error_message,
        )


def merge_quality_reports(
    base: FactorQualityReport,
    extra_metrics: tuple[FactorQualityMetric, ...],
) -> FactorQualityReport:
    metrics = tuple(_blocking_metric(metric) for metric in (*base.metrics, *extra_metrics))
    return FactorQualityReport(
        factor_run_id=base.factor_run_id,
        feature_set_id=base.feature_set_id,
        status=_pipeline_quality_status(metrics),
        metrics=metrics,
    )


def _blocking_metric(metric: FactorQualityMetric) -> FactorQualityMetric:
    if metric.severity != QualitySeverity.WARNING:
        return metric
    return replace(metric, severity=QualitySeverity.ERROR)


def _pipeline_quality_status(metrics: tuple[FactorQualityMetric, ...]) -> QualityStatus:
    if not metrics:
        return QualityStatus.FAILED
    if any(metric.severity != QualitySeverity.INFO for metric in metrics):
        return QualityStatus.FAILED
    return QualityStatus.PASSED
