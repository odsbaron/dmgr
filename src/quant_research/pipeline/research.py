from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

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
from quant_research.contracts.refs import DataRef
from quant_research.coverage.gates import CoverageGateError, CoverageGateProtocol
from quant_research.data.duckdb_store import LocalDuckDBStore, MarketDataStoreError
from quant_research.data.partition_contracts import MarketDataRef, ResolvedMarketData
from quant_research.data.resolver import MarketDataResolver
from quant_research.pipeline.bar_frame import bars_to_factor_frame
from quant_research.pipeline.contracts import (
    PipelineInputRefError,
    PipelineInputSlice,
    PipelineMarketDataError,
    PipelineUniverseError,
    ResearchRunRequest,
    ResearchRunResult,
    ResearchRunStatus,
    parse_pipeline_input_ref,
)
from quant_research.universe.contracts import ResolvedUniverse
from quant_research.universe.duckdb_store import UniverseStoreError
from quant_research.universe.resolver import UniverseResolver


@dataclass
class ResearchPipeline:
    data_store: LocalDuckDBStore
    factor_registry: FactorRegistry
    factor_runner: PolarsFactorRunner
    feature_store: LocalDuckDBFeatureStore
    quality_analyzer: FactorQualityAnalyzer
    coverage_gate: CoverageGateProtocol | None = None
    universe_resolver: UniverseResolver | None = None
    market_data_resolver: MarketDataResolver | None = None
    leakage_detector: PrefixInvarianceLeakageDetector = field(
        default_factory=PrefixInvarianceLeakageDetector
    )

    def run(self, request: ResearchRunRequest) -> ResearchRunResult:
        if request.coverage_report_ref is not None:
            if self.coverage_gate is None:
                return self._failed_result(
                    request,
                    error_step="validate_coverage",
                    error_code="COVERAGE_GATE_NOT_CONFIGURED",
                    error_message="coverage_report_ref requires a configured coverage gate",
                )
            try:
                self.coverage_gate.assert_report_consumable(request.coverage_report_ref)
            except CoverageGateError as exc:
                return self._failed_result(
                    request,
                    error_step="validate_coverage",
                    error_code=exc.code,
                    error_message=exc.message,
                )
            except ValueError as exc:
                return self._failed_result(
                    request,
                    error_step="validate_coverage",
                    error_code="COVERAGE_GATE_FAILED",
                    error_message=str(exc),
                )

        try:
            input_slice, resolved_market_data = self._resolve_market_data(request)
        except PipelineInputRefError as exc:
            return self._failed_result(
                request,
                error_step="parse_input_ref",
                error_code="INVALID_INPUT_DATA_REF",
                error_message=str(exc),
            )
        except PipelineMarketDataError as exc:
            return self._failed_result(
                request,
                error_step="resolve_market_data",
                error_code=exc.code,
                error_message=exc.message,
            )

        try:
            resolved_universe = self._resolve_universe(request)
        except PipelineUniverseError as exc:
            return self._failed_result(
                request,
                error_step="resolve_universe",
                error_code=exc.code,
                error_message=exc.message,
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

        config = self._factor_run_config(
            request,
            input_slice,
            resolved_universe,
            resolved_market_data,
        )

        try:
            self._validate_universe_against_bars(
                bars,
                request,
                resolved_universe,
                resolved_market_data,
            )
            compute_bars = self._filter_compute_bars(bars, request, resolved_universe)
            factor_frame = bars_to_factor_frame(compute_bars)
        except PipelineUniverseError as exc:
            return self._failed_result_with_manifest(
                request,
                config,
                error_step="validate_universe",
                error_code=exc.code,
                error_message=exc.message,
                row_count_input=len(bars),
            )
        except Exception as exc:
            return self._failed_result_with_manifest(
                request,
                config,
                error_step="build_factor_frame",
                error_code="BUILD_FACTOR_FRAME_FAILED",
                error_message=str(exc),
                row_count_input=len(bars),
            )

        try:
            resolved_factors = tuple(
                self.factor_registry.resolve_many(request.factor_ids, freq=input_slice.freq)
            )
        except Exception as exc:
            return self._failed_result_with_manifest(
                request,
                config,
                error_step="resolve_factors",
                error_code="RESOLVE_FACTORS_FAILED",
                error_message=str(exc),
                row_count_input=len(compute_bars),
            )

        try:
            computed_frame = self.factor_runner.run(factor_frame, config)
            factor_result_frame = self._crop_factor_output(
                computed_frame,
                request,
                resolved_universe,
            )
        except Exception as exc:
            return self._failed_result_with_manifest(
                request,
                config,
                error_step="compute_factors",
                error_code="COMPUTE_FACTORS_FAILED",
                error_message=str(exc),
                row_count_input=len(compute_bars),
                resolved_factors=resolved_factors,
            )

        commit = self.feature_store.commit_run(
            FeatureCommitRequest(
                config=config,
                factor_frame=factor_result_frame,
                resolved_factors=resolved_factors,
                input_row_count=len(compute_bars),
                allow_failed_overwrite=request.allow_failed_overwrite,
            )
        )
        if commit.status == FeatureRunStatus.FAILED:
            return self._failed_commit_result(request, commit, len(compute_bars))

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
            row_count_input=len(compute_bars),
            row_count_feature=commit.row_count_feature,
            row_count_snapshot=commit.row_count_snapshot,
            metric_count=len(merged_report.metrics),
        )

    def _resolve_universe(
        self,
        request: ResearchRunRequest,
    ) -> ResolvedUniverse | None:
        if request.universe_ref is None:
            return None
        if self.universe_resolver is None:
            raise PipelineUniverseError(
                "UNIVERSE_RESOLVER_NOT_CONFIGURED",
                "universe_ref requires a configured UniverseResolver",
            )
        try:
            return self.universe_resolver.resolve(request.universe_ref)
        except UniverseStoreError as exc:
            raise PipelineUniverseError(exc.code, exc.message) from exc
        except ValueError as exc:
            raise PipelineUniverseError("INVALID_UNIVERSE_REF", str(exc)) from exc

    def _factor_run_config(
        self,
        request: ResearchRunRequest,
        input_slice: PipelineInputSlice,
        resolved_universe: ResolvedUniverse | None,
        resolved_market_data: ResolvedMarketData | None,
    ) -> FactorRunConfig:
        return FactorRunConfig(
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
            universe_ref=(resolved_universe.universe_ref.uri if resolved_universe else None),
            universe_id=(resolved_universe.universe_id if resolved_universe else None),
            universe_version=(resolved_universe.universe_version if resolved_universe else None),
            universe_definition_hash=(
                resolved_universe.definition_hash if resolved_universe else None
            ),
            universe_snapshot_set_hash=(
                resolved_universe.snapshot_set_hash if resolved_universe else None
            ),
            market_data_ref=(
                resolved_market_data.market_data_ref.uri if resolved_market_data else None
            ),
            market_dataset_version=(
                resolved_market_data.dataset_version if resolved_market_data else None
            ),
            market_data_definition_hash=(
                resolved_market_data.definition_hash if resolved_market_data else None
            ),
            market_data_snapshot_set_hash=(
                resolved_market_data.snapshot_set_hash if resolved_market_data else None
            ),
        )

    def _resolve_market_data(
        self,
        request: ResearchRunRequest,
    ) -> tuple[PipelineInputSlice, ResolvedMarketData | None]:
        try:
            data_ref = DataRef.parse(request.input_data_ref)
        except ValueError as exc:
            raise PipelineInputRefError(str(exc)) from exc
        if "snapshot_set_id" not in data_ref.filters:
            return parse_pipeline_input_ref(request), None

        resolver = self.market_data_resolver or MarketDataResolver(self.data_store)
        try:
            resolved = resolver.resolve(MarketDataRef.parse(request.input_data_ref))
        except MarketDataStoreError as exc:
            raise PipelineMarketDataError(exc.code, exc.message) from exc
        except ValueError as exc:
            raise PipelineMarketDataError("INVALID_MARKET_DATA_REF", str(exc)) from exc
        return (
            PipelineInputSlice(
                data_ref=data_ref,
                dataset_id=resolved.dataset_id,
                dataset_version=resolved.dataset_version,
                freq=resolved.freq,
            ),
            resolved,
        )

    def _validate_universe_against_bars(
        self,
        bars: list[BarRecord],
        request: ResearchRunRequest,
        resolved_universe: ResolvedUniverse | None,
        resolved_market_data: ResolvedMarketData | None,
    ) -> None:
        if resolved_universe is None:
            return
        output_bars = [bar for bar in bars if self._is_output_bar(bar, request)]
        output_dates = (
            self._market_output_dates(resolved_market_data, request)
            if resolved_market_data is not None
            else {bar.trading_date for bar in output_bars}
        )
        missing_dates = output_dates - set(resolved_universe.members_by_date)
        if missing_dates:
            rendered = ", ".join(value.isoformat() for value in sorted(missing_dates))
            raise PipelineUniverseError(
                "UNIVERSE_DATE_NOT_COVERED",
                f"Universe snapshot set does not cover market-data dates: {rendered}",
            )
        if (
            resolved_market_data is not None
            and resolved_market_data.asset_class != resolved_universe.asset_class
        ):
            raise PipelineUniverseError(
                "UNIVERSE_ASSET_CLASS_MISMATCH",
                "Universe asset class does not match market-data definition",
            )
        if resolved_market_data is None:
            mismatched = {
                bar.asset_class.value
                for bar in output_bars
                if bar.symbol in resolved_universe.instrument_ids
                and bar.asset_class != resolved_universe.asset_class
            }
            if mismatched:
                raise PipelineUniverseError(
                    "UNIVERSE_ASSET_CLASS_MISMATCH",
                    "Universe asset class does not match member market data",
                )

    def _market_output_dates(
        self,
        resolved_market_data: ResolvedMarketData,
        request: ResearchRunRequest,
    ) -> set[date]:
        zone = ZoneInfo(resolved_market_data.timezone)

        def local_date(value: datetime | None) -> date | None:
            if value is None:
                return None
            if value.tzinfo is None or value.utcoffset() is None:
                return value.date()
            return value.astimezone(zone).date()

        start = local_date(request.as_of_start)
        end = local_date(request.as_of_end)
        return {
            trading_date
            for trading_date in resolved_market_data.trading_dates
            if (start is None or trading_date >= start) and (end is None or trading_date <= end)
        }

    def _filter_compute_bars(
        self,
        bars: list[BarRecord],
        request: ResearchRunRequest,
        resolved_universe: ResolvedUniverse | None,
    ) -> list[BarRecord]:
        filtered = bars
        if resolved_universe is not None:
            instrument_ids = resolved_universe.instrument_ids
            filtered = [bar for bar in filtered if bar.symbol in instrument_ids]
        if request.symbols is not None:
            symbols = set(request.symbols)
            filtered = [bar for bar in filtered if bar.symbol in symbols]
        if request.as_of_end is not None:
            filtered = [bar for bar in filtered if bar.bar_end_time <= request.as_of_end]
        return filtered

    def _crop_factor_output(
        self,
        frame: pl.LazyFrame,
        request: ResearchRunRequest,
        resolved_universe: ResolvedUniverse | None,
    ) -> pl.LazyFrame:
        result = frame
        if request.as_of_start is not None:
            result = result.filter(pl.col("as_of") >= pl.lit(request.as_of_start))
        if request.as_of_end is not None:
            result = result.filter(pl.col("as_of") <= pl.lit(request.as_of_end))
        if resolved_universe is not None:
            rows = [
                {"trading_date": trading_date.isoformat(), "symbol": instrument_id}
                for trading_date, members in resolved_universe.members_by_date.items()
                for instrument_id in sorted(members)
            ]
            membership = pl.DataFrame(
                rows,
                schema={"trading_date": pl.String, "symbol": pl.String},
            ).lazy()
            result = result.join(
                membership,
                on=["trading_date", "symbol"],
                how="inner",
            )
        return result

    def _is_output_bar(self, bar: BarRecord, request: ResearchRunRequest) -> bool:
        if request.as_of_start is not None and bar.bar_end_time < request.as_of_start:
            return False
        if request.as_of_end is not None and bar.bar_end_time > request.as_of_end:
            return False
        return True

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
        manifest_ref: DataRef | None = None,
    ) -> ResearchRunResult:
        return ResearchRunResult.from_quality_status(
            factor_run_id=request.factor_run_id,
            status=ResearchRunStatus.FAILED,
            feature_table_ref=None,
            snapshot_ref=None,
            manifest_ref=manifest_ref,
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

    def _failed_result_with_manifest(
        self,
        request: ResearchRunRequest,
        config: FactorRunConfig,
        *,
        error_step: str,
        error_code: str,
        error_message: str,
        row_count_input: int,
        resolved_factors=(),
    ) -> ResearchRunResult:
        commit = self.feature_store.commit_failed_run(
            config,
            error_code=error_code,
            error_message=error_message,
            input_row_count=row_count_input,
            resolved_factors=resolved_factors,
        )
        return self._failed_result(
            request,
            error_step=error_step,
            error_code=error_code,
            error_message=error_message,
            row_count_input=row_count_input,
            manifest_ref=commit.manifest_ref,
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
