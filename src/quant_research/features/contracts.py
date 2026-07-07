from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import polars as pl

from quant_research.contracts.refs import DataRef
from quant_research.factors.contracts import FactorRunConfig
from quant_research.factors.registry import RegisteredFactor


class FeatureRunStatus(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class FeatureStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class FeatureCommitRequest:
    config: FactorRunConfig
    factor_frame: pl.LazyFrame
    resolved_factors: tuple[RegisteredFactor, ...]
    input_row_count: int | None = None
    allow_failed_overwrite: bool = False


@dataclass(frozen=True)
class FeatureCommitResult:
    factor_run_id: str
    status: FeatureRunStatus
    snapshot_ref: DataRef | None
    feature_table_ref: DataRef
    manifest_ref: DataRef
    row_count_feature: int
    row_count_snapshot: int
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class FeatureRunManifest:
    factor_run_id: str
    feature_set_id: str
    dataset_id: str
    freq: str
    input_data_refs: tuple[str, ...]
    factor_versions: dict[str, str]
    factor_output_fields: dict[str, tuple[str, ...]]
    engine: str
    execution_mode: str
    status: FeatureRunStatus
    started_at: str
    finished_at: str | None
    row_count_input: int | None
    row_count_feature: int
    row_count_snapshot: int
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class FeatureValue:
    factor_run_id: str
    feature_set_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    factor_id: str
    factor_version: str
    output_field: str
    value_float: float | None
    value_string: str | None
    value_kind: str
    warmup_complete: bool
    quality_flags: tuple[str, ...]
    input_data_ref: str
    created_at: str

    @property
    def value(self) -> object:
        if self.value_kind == "null":
            return None
        if self.value_kind == "float":
            return self.value_float
        if self.value_kind == "bool":
            return self.value_string == "true"
        return self.value_string


@dataclass(frozen=True)
class FeatureSnapshot:
    snapshot_id: str
    feature_set_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    features: dict[str, object]
    factor_run_ids: tuple[str, ...]
    input_data_refs: tuple[str, ...]
    warmup_complete: bool
    quality_flags: tuple[str, ...]
    feature_ref: str
    created_at: str
