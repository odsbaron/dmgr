from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from quant_research.contracts.bar import Frequency
from quant_research.contracts.refs import DataRef
from quant_research.features.leakage import PrefixProbeConfig
from quant_research.features.quality import QualityStatus


class ResearchRunStatus(StrEnum):
    COMMITTED = "COMMITTED"
    QUALITY_FAILED = "QUALITY_FAILED"
    FAILED = "FAILED"


class PipelineInputRefError(ValueError):
    pass


class PipelineUniverseError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class PipelineMarketDataError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PipelineInputSlice:
    data_ref: DataRef
    dataset_id: str
    freq: Frequency
    dataset_version: str | None = None


@dataclass(frozen=True)
class ResearchRunRequest:
    factor_run_id: str
    feature_set_id: str
    input_data_ref: str
    factor_ids: tuple[str, ...]
    coverage_report_ref: str | None = None
    universe_ref: str | None = None
    symbols: tuple[str, ...] | None = None
    as_of_start: datetime | None = None
    as_of_end: datetime | None = None
    allow_failed_overwrite: bool = False
    prefix_probe_config: PrefixProbeConfig | None = None
    engine: str = "polars"
    execution_mode: str = "lazy"
    seed: int | None = None

    def __post_init__(self) -> None:
        if not self.factor_run_id:
            raise ValueError("factor_run_id is required")
        if not self.feature_set_id:
            raise ValueError("feature_set_id is required")
        if not self.input_data_ref:
            raise ValueError("input_data_ref is required")
        if not self.factor_ids:
            raise ValueError("factor_ids must not be empty")
        if self.coverage_report_ref is not None and not self.coverage_report_ref.strip():
            raise ValueError("coverage_report_ref must not be empty")
        if self.universe_ref is not None and not self.universe_ref.strip():
            raise ValueError("universe_ref must not be empty")
        if self.symbols == ():
            raise ValueError("symbols must not be empty")
        if (
            self.as_of_start is not None
            and self.as_of_end is not None
            and self.as_of_start > self.as_of_end
        ):
            raise ValueError("as_of_start must be <= as_of_end")


@dataclass(frozen=True)
class ResearchRunResult:
    factor_run_id: str
    status: ResearchRunStatus
    feature_table_ref: DataRef | None
    snapshot_ref: DataRef | None
    manifest_ref: DataRef | None
    quality_status: QualityStatus
    quality_summary: dict[str, Any]
    consumable: bool
    block_reason: str | None
    row_count_input: int
    row_count_feature: int
    row_count_snapshot: int
    metric_count: int
    error_step: str | None = None
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_quality_status(
        cls,
        *,
        factor_run_id: str,
        status: ResearchRunStatus,
        quality_status: QualityStatus,
        quality_summary: dict[str, Any],
        row_count_input: int,
        row_count_feature: int,
        row_count_snapshot: int,
        metric_count: int,
        feature_table_ref: DataRef | None = None,
        snapshot_ref: DataRef | None = None,
        manifest_ref: DataRef | None = None,
        error_step: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> "ResearchRunResult":
        consumable = (
            status == ResearchRunStatus.COMMITTED and quality_status == QualityStatus.PASSED
        )
        block_reason = None
        if not consumable:
            block_reason = (
                "pipeline_failed" if status == ResearchRunStatus.FAILED else "quality_failed"
            )
        return cls(
            factor_run_id=factor_run_id,
            status=status,
            feature_table_ref=feature_table_ref,
            snapshot_ref=snapshot_ref,
            manifest_ref=manifest_ref,
            quality_status=quality_status,
            quality_summary=quality_summary,
            consumable=consumable,
            block_reason=block_reason,
            row_count_input=row_count_input,
            row_count_feature=row_count_feature,
            row_count_snapshot=row_count_snapshot,
            metric_count=metric_count,
            error_step=error_step,
            error_code=error_code,
            error_message=error_message,
        )


def parse_pipeline_input_ref(request: ResearchRunRequest) -> PipelineInputSlice:
    try:
        data_ref = DataRef.parse(request.input_data_ref)
    except ValueError as exc:
        raise PipelineInputRefError(str(exc)) from exc

    if data_ref.table != "curated_market_bar":
        raise PipelineInputRefError("input_data_ref must point to curated_market_bar")

    dataset_id = data_ref.filters.get("dataset_id")
    if not dataset_id:
        raise PipelineInputRefError("input_data_ref requires dataset_id filter")

    freq_value = data_ref.filters.get("freq")
    if not freq_value:
        raise PipelineInputRefError("input_data_ref requires freq filter")
    try:
        freq = Frequency(freq_value)
    except ValueError as exc:
        raise PipelineInputRefError(f"unsupported input_data_ref freq: {freq_value}") from exc

    return PipelineInputSlice(data_ref=data_ref, dataset_id=dataset_id, freq=freq)
