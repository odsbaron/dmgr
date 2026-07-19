from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from quant_research.contracts.bar import BarRecord
from quant_research.contracts.refs import DataRef
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.duckdb_store import MarketDataStoreError
from quant_research.data.partition_contracts import MarketDataRef, ResolvedMarketData
from quant_research.data.resolver import MarketDataResolver
from quant_research.features.quality import QualityStatus
from quant_research.labels.contracts import LabelCommitRequest
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.generation import (
    ForwardReturnLabelConfig,
    forward_return_labels_from_bars,
)
from quant_research.labels.quality import LabelQualityAnalyzer


class LabelRunStatus(StrEnum):
    COMMITTED = "COMMITTED"
    QUALITY_FAILED = "QUALITY_FAILED"
    FAILED = "FAILED"


class LabelPipelineInputRefError(ValueError):
    pass


@dataclass(frozen=True)
class LabelRunRequest:
    label_run_id: str
    label_set_id: str
    source_ref: str
    label_id: str
    label_version: str
    forward_bars: int

    def __post_init__(self) -> None:
        if not self.label_run_id:
            raise ValueError("label_run_id is required")
        if not self.label_set_id:
            raise ValueError("label_set_id is required")
        if not self.source_ref:
            raise ValueError("source_ref is required")
        if not self.label_id:
            raise ValueError("label_id is required")
        if not self.label_version:
            raise ValueError("label_version is required")
        if self.forward_bars < 1:
            raise ValueError("forward_bars must be >= 1")


@dataclass(frozen=True)
class LabelRunResult:
    label_run_id: str
    status: LabelRunStatus
    label_ref: DataRef | None
    manifest_ref: DataRef | None
    quality_status: QualityStatus
    quality_summary: dict[str, Any]
    consumable: bool
    block_reason: str | None
    row_count_label: int
    metric_count: int
    error_step: str | None = None
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_quality_status(
        cls,
        *,
        label_run_id: str,
        status: LabelRunStatus,
        quality_status: QualityStatus,
        quality_summary: dict[str, Any],
        row_count_label: int,
        metric_count: int,
        label_ref: DataRef | None = None,
        manifest_ref: DataRef | None = None,
        error_step: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> "LabelRunResult":
        consumable = status == LabelRunStatus.COMMITTED and quality_status == QualityStatus.PASSED
        block_reason = None
        if not consumable:
            block_reason = "pipeline_failed" if status == LabelRunStatus.FAILED else "quality_failed"
        return cls(
            label_run_id=label_run_id,
            status=status,
            label_ref=label_ref,
            manifest_ref=manifest_ref,
            quality_status=quality_status,
            quality_summary=quality_summary,
            consumable=consumable,
            block_reason=block_reason,
            row_count_label=row_count_label,
            metric_count=metric_count,
            error_step=error_step,
            error_code=error_code,
            error_message=error_message,
        )


@dataclass
class LabelPipeline:
    data_store: LocalDuckDBStore
    label_store: LocalDuckDBLabelStore
    quality_analyzer: LabelQualityAnalyzer
    market_data_resolver: MarketDataResolver | None = None

    def run(self, request: LabelRunRequest) -> LabelRunResult:
        try:
            source_ref = parse_label_source_ref(request)
        except LabelPipelineInputRefError as exc:
            return self._failed_result(
                request,
                error_step="parse_source_ref",
                error_code="INVALID_SOURCE_REF",
                error_message=str(exc),
            )

        try:
            resolved_market_data = self._resolve_market_data(source_ref)
        except (MarketDataStoreError, ValueError) as exc:
            return self._failed_result(
                request,
                error_step="resolve_market_data",
                error_code=getattr(exc, "code", "INVALID_MARKET_DATA_REF"),
                error_message=str(exc),
            )

        try:
            bars = self.data_store.read_bars(source_ref)
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="read_bars",
                error_code="READ_BARS_FAILED",
                error_message=str(exc),
            )

        try:
            commit_request = self._label_commit_request(
                request,
                source_ref,
                bars,
                resolved_market_data,
            )
            label_ref = self.label_store.commit_labels(commit_request)
        except Exception as exc:
            return self._failed_result(
                request,
                error_step="commit_labels",
                error_code="COMMIT_LABELS_FAILED",
                error_message=str(exc),
            )

        labels = self.label_store.read_labels(label_ref)
        report = self.quality_analyzer.analyze(tuple(labels))
        self.label_store.commit_quality_report(report)
        status = (
            LabelRunStatus.COMMITTED
            if report.status == QualityStatus.PASSED
            else LabelRunStatus.QUALITY_FAILED
        )
        return LabelRunResult.from_quality_status(
            label_run_id=request.label_run_id,
            status=status,
            label_ref=label_ref,
            manifest_ref=DataRef("label_run_manifest", {"label_run_id": request.label_run_id}),
            quality_status=report.status,
            quality_summary=report.summary,
            row_count_label=len(labels),
            metric_count=len(report.metrics),
        )

    def _label_commit_request(
        self,
        request: LabelRunRequest,
        source_ref: DataRef,
        bars: list[BarRecord],
        resolved_market_data: ResolvedMarketData | None,
    ) -> LabelCommitRequest:
        return forward_return_labels_from_bars(
            bars,
            ForwardReturnLabelConfig(
                label_run_id=request.label_run_id,
                label_set_id=request.label_set_id,
                label_id=request.label_id,
                label_version=request.label_version,
                forward_bars=request.forward_bars,
                source_id=source_ref.uri,
                source_ref=source_ref.uri,
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
            ),
        )

    def _resolve_market_data(self, source_ref: DataRef) -> ResolvedMarketData | None:
        if "snapshot_set_id" not in source_ref.filters:
            return None
        resolver = self.market_data_resolver or MarketDataResolver(self.data_store)
        return resolver.resolve(MarketDataRef.parse(source_ref.uri))

    def _failed_result(
        self,
        request: LabelRunRequest,
        *,
        error_step: str,
        error_code: str,
        error_message: str,
    ) -> LabelRunResult:
        return LabelRunResult.from_quality_status(
            label_run_id=request.label_run_id,
            status=LabelRunStatus.FAILED,
            label_ref=None,
            manifest_ref=None,
            quality_status=QualityStatus.NOT_RUN,
            quality_summary={},
            row_count_label=0,
            metric_count=0,
            error_step=error_step,
            error_code=error_code,
            error_message=error_message,
        )


def parse_label_source_ref(request: LabelRunRequest) -> DataRef:
    try:
        source_ref = DataRef.parse(request.source_ref)
    except ValueError as exc:
        raise LabelPipelineInputRefError(str(exc)) from exc

    if source_ref.table != "curated_market_bar":
        raise LabelPipelineInputRefError("source_ref must point to curated_market_bar")
    return source_ref
