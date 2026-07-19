from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class LabelStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class LabelSourceKind(StrEnum):
    MARKET_DATA = "MARKET_DATA"
    FACTOR_RUN = "FACTOR_RUN"
    LEGACY = "LEGACY"


@dataclass(frozen=True)
class LabelValue:
    label_run_id: str
    label_set_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    label_id: str
    label_version: str
    value_float: float | None
    value_string: str | None
    value_kind: str
    forward_bars: int
    source_factor_run_id: str
    created_at: str
    source_kind: LabelSourceKind = LabelSourceKind.LEGACY
    source_ref: str | None = None

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
class LabelCommitRequest:
    label_run_id: str
    label_set_id: str
    source_factor_run_id: str
    labels: tuple[LabelValue, ...]
    source_kind: LabelSourceKind = LabelSourceKind.LEGACY
    source_ref: str | None = None
    dataset_id: str | None = None
    freq: str | None = None
    forward_bars: int | None = None
    source_as_of_start: str | None = None
    source_as_of_end: str | None = None
    market_data_ref: str | None = None
    market_dataset_version: str | None = None
    market_data_definition_hash: str | None = None
    market_data_snapshot_set_hash: str | None = None
    universe_ref: str | None = None
    universe_id: str | None = None
    universe_version: str | None = None
    universe_definition_hash: str | None = None
    universe_snapshot_set_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.label_run_id:
            raise ValueError("label_run_id is required")
        if not self.label_set_id:
            raise ValueError("label_set_id is required")
        if self.source_kind == LabelSourceKind.FACTOR_RUN and not self.source_factor_run_id:
            raise ValueError("factor-run labels require source_factor_run_id")
        if self.source_kind == LabelSourceKind.MARKET_DATA and not self.source_ref:
            raise ValueError("market-data labels require source_ref")
        if self.forward_bars is not None and self.forward_bars < 1:
            raise ValueError("forward_bars must be >= 1")
        _validate_complete_lineage(
            "market-data",
            self.market_data_ref,
            (
                self.market_dataset_version,
                self.market_data_definition_hash,
                self.market_data_snapshot_set_hash,
            ),
        )
        _validate_complete_lineage(
            "Universe",
            self.universe_ref,
            (
                self.universe_id,
                self.universe_version,
                self.universe_definition_hash,
                self.universe_snapshot_set_hash,
            ),
        )


@dataclass(frozen=True)
class LabelRunManifest:
    label_run_id: str
    label_set_id: str
    source_factor_run_id: str
    row_count_label: int
    status: str
    created_at: str
    quality_status: str = "NOT_RUN"
    quality_summary: dict[str, Any] = field(default_factory=dict)
    source_kind: LabelSourceKind = LabelSourceKind.LEGACY
    source_ref: str | None = None
    dataset_id: str | None = None
    freq: str | None = None
    forward_bars: int | None = None
    source_as_of_start: str | None = None
    source_as_of_end: str | None = None
    market_data_ref: str | None = None
    market_dataset_version: str | None = None
    market_data_definition_hash: str | None = None
    market_data_snapshot_set_hash: str | None = None
    universe_ref: str | None = None
    universe_id: str | None = None
    universe_version: str | None = None
    universe_definition_hash: str | None = None
    universe_snapshot_set_hash: str | None = None


def _validate_complete_lineage(
    name: str,
    ref: str | None,
    values: tuple[str | None, ...],
) -> None:
    if ref is None and any(value is not None for value in values):
        raise ValueError(f"{name} lineage requires a ref")
    if ref is not None and any(value is None for value in values):
        raise ValueError(f"{name} ref requires complete lineage")
