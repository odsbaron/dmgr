from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import isfinite


class SignalContractError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class PortfolioSelectionMode(StrEnum):
    TOP_K = "TOP_K"
    TOP_QUANTILE = "TOP_QUANTILE"


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SignalContractError("NAIVE_TIMESTAMP", f"{name} must be timezone-aware")


@dataclass(frozen=True)
class AlphaScore:
    score_run_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: datetime
    available_at: datetime
    score: float
    source_ref: str

    def __post_init__(self) -> None:
        for name in ("score_run_id", "dataset_id", "symbol", "freq", "source_ref"):
            if not getattr(self, name):
                raise SignalContractError("MISSING_FIELD", f"{name} is required")
        _require_aware("as_of", self.as_of)
        _require_aware("available_at", self.available_at)
        if self.available_at < self.as_of:
            raise SignalContractError(
                "INVALID_AVAILABILITY",
                "available_at must be greater than or equal to as_of",
            )
        if not isfinite(self.score):
            raise SignalContractError("INVALID_SCORE", "score must be finite")


@dataclass(frozen=True)
class PortfolioConstructionConfig:
    portfolio_run_id: str
    selection_mode: PortfolioSelectionMode = PortfolioSelectionMode.TOP_K
    top_k: int = 10
    quantile_count: int = 5
    target_quantile: int = 5
    gross_exposure: float = 1.0

    def __post_init__(self) -> None:
        if not self.portfolio_run_id:
            raise SignalContractError("MISSING_PORTFOLIO_RUN_ID", "portfolio_run_id is required")
        if self.top_k < 1:
            raise SignalContractError("INVALID_TOP_K", "top_k must be >= 1")
        if self.quantile_count < 2:
            raise SignalContractError("INVALID_QUANTILE_COUNT", "quantile_count must be >= 2")
        if not 1 <= self.target_quantile <= self.quantile_count:
            raise SignalContractError(
                "INVALID_TARGET_QUANTILE",
                "target_quantile must be between 1 and quantile_count",
            )
        if not 0 < self.gross_exposure <= 1:
            raise SignalContractError("INVALID_GROSS_EXPOSURE", "gross_exposure must be in (0, 1]")


@dataclass(frozen=True)
class TargetWeight:
    portfolio_run_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: datetime
    available_at: datetime
    target_weight: float
    source_score_ref: str

    def __post_init__(self) -> None:
        for name in (
            "portfolio_run_id",
            "dataset_id",
            "symbol",
            "freq",
            "source_score_ref",
        ):
            if not getattr(self, name):
                raise SignalContractError("MISSING_FIELD", f"{name} is required")
        _require_aware("as_of", self.as_of)
        _require_aware("available_at", self.available_at)
        if self.available_at < self.as_of:
            raise SignalContractError(
                "INVALID_AVAILABILITY",
                "available_at must be greater than or equal to as_of",
            )
        if not isfinite(self.target_weight) or not 0 <= self.target_weight <= 1:
            raise SignalContractError(
                "INVALID_TARGET_WEIGHT", "target_weight must be finite and in [0, 1]"
            )
