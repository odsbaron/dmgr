from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from quant_research.contracts.bar import Frequency


class ComputeMode(StrEnum):
    OPERATOR_GRAPH = "operator_graph"
    POLARS_EXPR = "polars_expr"
    FRAME_TRANSFORM = "frame_transform"
    PYTHON_UDF = "python_udf"


@dataclass(frozen=True)
class FactorContext:
    input_data_ref: str
    dataset_id: str
    freq: Frequency
    as_of_start: datetime | None = None
    as_of_end: datetime | None = None
    symbols: tuple[str, ...] | None = None

    @classmethod
    def from_run_config(cls, config: "FactorRunConfig") -> "FactorContext":
        return cls(
            input_data_ref=config.input_data_ref,
            dataset_id=config.dataset_id,
            freq=config.freq,
            as_of_start=config.as_of_start,
            as_of_end=config.as_of_end,
            symbols=config.symbols,
        )


@dataclass(frozen=True)
class FactorSpec:
    factor_id: str
    version: str
    namespace: str
    description: str
    input_fields: tuple[str, ...]
    output_fields: tuple[str, ...]
    supported_freqs: tuple[Frequency, ...]
    lookback_bars: int
    warmup_bars: int
    compute_mode: ComputeMode
    params_schema: dict[str, Any] = field(default_factory=dict)
    output_dtype: dict[str, str] = field(default_factory=dict)
    quality_rules: dict[str, Any] = field(default_factory=dict)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.factor_id:
            raise ValueError("factor_id is required")
        if not self.version:
            raise ValueError("version is required")
        if not self.input_fields:
            raise ValueError("input_fields must not be empty")
        if not self.output_fields:
            raise ValueError("output_fields must not be empty")
        if not self.supported_freqs:
            raise ValueError("supported_freqs must not be empty")
        if self.lookback_bars < 1:
            raise ValueError("lookback_bars must be >= 1")
        if self.warmup_bars < 0:
            raise ValueError("warmup_bars must be >= 0")


@dataclass(frozen=True)
class FactorRunConfig:
    factor_run_id: str
    feature_set_id: str
    input_data_ref: str
    factor_ids: tuple[str, ...]
    freq: Frequency
    dataset_id: str
    as_of_start: datetime | None = None
    as_of_end: datetime | None = None
    symbols: tuple[str, ...] | None = None
    engine: str = "polars"
    execution_mode: str = "lazy"
    strict_quality: bool = True
    seed: int | None = None
    universe_ref: str | None = None
    universe_id: str | None = None
    universe_version: str | None = None
    universe_definition_hash: str | None = None
    universe_snapshot_set_hash: str | None = None
    market_data_ref: str | None = None
    market_dataset_version: str | None = None
    market_data_definition_hash: str | None = None
    market_data_snapshot_set_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.factor_run_id:
            raise ValueError("factor_run_id is required")
        if not self.feature_set_id:
            raise ValueError("feature_set_id is required")
        if not self.input_data_ref.startswith("duckdb://"):
            raise ValueError("input_data_ref must be a duckdb DataRef")
        if not self.factor_ids:
            raise ValueError("factor_ids must not be empty")
        lineage = (
            self.universe_id,
            self.universe_version,
            self.universe_definition_hash,
            self.universe_snapshot_set_hash,
        )
        if self.universe_ref is None and any(value is not None for value in lineage):
            raise ValueError("Universe lineage requires universe_ref")
        if self.universe_ref is not None and any(value is None for value in lineage):
            raise ValueError("universe_ref requires complete Universe lineage")
        market_data_lineage = (
            self.market_dataset_version,
            self.market_data_definition_hash,
            self.market_data_snapshot_set_hash,
        )
        if self.market_data_ref is None and any(value is not None for value in market_data_lineage):
            raise ValueError("market-data lineage requires market_data_ref")
        if self.market_data_ref is not None and any(value is None for value in market_data_lineage):
            raise ValueError("market_data_ref requires complete market-data lineage")
