from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WalkForwardWindowMode(StrEnum):
    ROLLING = "ROLLING"
    EXPANDING = "EXPANDING"


class WalkForwardError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class WalkForwardSplitPlan:
    train_periods: int
    test_periods: int
    step_periods: int | None = None
    purge_periods: int = 0
    embargo_periods: int = 0
    window_mode: WalkForwardWindowMode = WalkForwardWindowMode.ROLLING

    def __post_init__(self) -> None:
        try:
            mode = WalkForwardWindowMode(self.window_mode)
        except ValueError as exc:
            raise WalkForwardError(
                "INVALID_SPLIT_PLAN",
                f"unsupported window mode: {self.window_mode}",
            ) from exc
        object.__setattr__(self, "window_mode", mode)

        if self.train_periods <= 0 or self.test_periods <= 0:
            raise WalkForwardError(
                "INVALID_SPLIT_PLAN",
                "train_periods and test_periods must be positive",
            )
        if self.resolved_step_periods <= 0:
            raise WalkForwardError(
                "INVALID_SPLIT_PLAN",
                "step_periods must be positive",
            )
        if self.purge_periods < 0 or self.embargo_periods < 0:
            raise WalkForwardError(
                "INVALID_SPLIT_PLAN",
                "purge_periods and embargo_periods must not be negative",
            )
        if self.resolved_step_periods < self.test_periods:
            raise WalkForwardError(
                "OVERLAPPING_OOS_WINDOWS",
                "step_periods must be greater than or equal to test_periods",
            )

    @property
    def resolved_step_periods(self) -> int:
        return self.step_periods if self.step_periods is not None else self.test_periods


@dataclass(frozen=True)
class WalkForwardFold:
    fold_id: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    train_period_count: int
    test_period_count: int
    purge_start: str | None = None
    purge_end: str | None = None
    embargo_start: str | None = None
    embargo_end: str | None = None


@dataclass(frozen=True)
class OOSAssignment:
    fold_id: str
    as_of: str


@dataclass(frozen=True)
class WalkForwardSplitResult:
    materialized_dataset_id: str
    plan: WalkForwardSplitPlan
    folds: tuple[WalkForwardFold, ...]
    oos_assignments: tuple[OOSAssignment, ...]
