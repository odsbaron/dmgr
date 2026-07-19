from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from quant_research.contracts.bar import BarRecord, Frequency
from quant_research.contracts.refs import DataRef
from quant_research.signals.contracts import TargetWeight


class BacktestError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class BacktestConflictError(BacktestError):
    pass


class BacktestRunStatus(StrEnum):
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


def as_decimal(value: Decimal | str | float | int) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class ProportionalCostConfig:
    buy_rate: Decimal = Decimal("0")
    sell_rate: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        object.__setattr__(self, "buy_rate", as_decimal(self.buy_rate))
        object.__setattr__(self, "sell_rate", as_decimal(self.sell_rate))
        if (
            not self.buy_rate.is_finite()
            or not self.sell_rate.is_finite()
            or self.buy_rate < 0
            or self.sell_rate < 0
        ):
            raise BacktestError("INVALID_COST_RATE", "cost rates must be non-negative")

    def calculate(self, side: Side, notional: Decimal) -> Decimal:
        rate = self.buy_rate if side == Side.BUY else self.sell_rate
        return notional * rate


@dataclass(frozen=True)
class DailyExecutionConfig:
    price_field: str = "open"
    lot_size: int = 1
    convention: str = "NEXT_ELIGIBLE_DAILY_OPEN"

    def __post_init__(self) -> None:
        if self.price_field != "open":
            raise BacktestError("UNSUPPORTED_PRICE_FIELD", "daily MVP supports open execution only")
        if self.lot_size < 1:
            raise BacktestError("INVALID_LOT_SIZE", "lot_size must be >= 1")


@dataclass(frozen=True)
class DailyBacktestRequest:
    backtest_run_id: str
    target_source_ref: str
    market_data_ref: str
    target_weights: tuple[TargetWeight, ...]
    bars: tuple[BarRecord, ...]
    initial_cash: Decimal = Decimal("1000000")
    execution: DailyExecutionConfig = field(default_factory=DailyExecutionConfig)
    costs: ProportionalCostConfig = field(default_factory=ProportionalCostConfig)
    universe_ref: str | None = None
    calendar_ref: str | None = None
    daily_status_ref: str | None = None
    coverage_report_ref: str | None = None

    def __post_init__(self) -> None:
        if not self.backtest_run_id:
            raise BacktestError("MISSING_RUN_ID", "backtest_run_id is required")
        for name in ("target_source_ref", "market_data_ref"):
            value = getattr(self, name)
            if not value:
                raise BacktestError("MISSING_REF", f"{name} is required")
            try:
                DataRef.parse(value)
            except ValueError as exc:
                raise BacktestError("INVALID_REF", f"invalid {name}: {exc}") from exc
        for name in (
            "universe_ref",
            "calendar_ref",
            "daily_status_ref",
            "coverage_report_ref",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            try:
                DataRef.parse(value)
            except ValueError as exc:
                raise BacktestError("INVALID_REF", f"invalid {name}: {exc}") from exc
        if not self.target_weights:
            raise BacktestError("EMPTY_TARGETS", "target_weights must not be empty")
        if not self.bars:
            raise BacktestError("EMPTY_BARS", "bars must not be empty")
        object.__setattr__(self, "initial_cash", as_decimal(self.initial_cash))
        if self.initial_cash <= 0:
            raise BacktestError("INVALID_INITIAL_CASH", "initial_cash must be positive")
        if any(bar.freq != Frequency.D1 for bar in self.bars):
            raise BacktestError("UNSUPPORTED_FREQUENCY", "daily backtest requires 1d bars")
        target_datasets = {target.dataset_id for target in self.target_weights}
        target_freqs = {target.freq for target in self.target_weights}
        portfolio_run_ids = {target.portfolio_run_id for target in self.target_weights}
        if len(target_datasets) != 1 or target_freqs != {Frequency.D1.value}:
            raise BacktestError(
                "MIXED_TARGET_SCOPE", "target weights must share one dataset and daily frequency"
            )
        if len(portfolio_run_ids) != 1:
            raise BacktestError(
                "MIXED_PORTFOLIO_RUNS", "target weights must share one portfolio_run_id"
            )
        totals: dict[datetime, float] = {}
        seen: set[tuple[datetime, str]] = set()
        for target in self.target_weights:
            key = (target.as_of, target.symbol)
            if key in seen:
                raise BacktestError(
                    "DUPLICATE_TARGET_KEY", "target weights contain duplicate as_of/symbol keys"
                )
            seen.add(key)
            totals[target.as_of] = totals.get(target.as_of, 0.0) + target.target_weight
        if any(total > 1.0 + 1e-12 for total in totals.values()):
            raise BacktestError(
                "TARGET_EXPOSURE_EXCEEDED", "target weights must sum to at most one per as_of"
            )


@dataclass(frozen=True)
class Fill:
    fill_id: str
    backtest_run_id: str
    rebalance_as_of: datetime
    execution_time: datetime
    trading_date: date
    symbol: str
    side: Side
    quantity: int
    price: Decimal
    notional: Decimal
    cost: Decimal


@dataclass(frozen=True)
class PositionSnapshot:
    backtest_run_id: str
    trading_date: date
    as_of: datetime
    symbol: str
    quantity: int
    close_price: Decimal
    market_value: Decimal
    portfolio_weight: float


@dataclass(frozen=True)
class NavSnapshot:
    backtest_run_id: str
    trading_date: date
    as_of: datetime
    cash: Decimal
    market_value: Decimal
    nav: Decimal


@dataclass(frozen=True)
class BacktestMetric:
    backtest_run_id: str
    metric_name: str
    metric_value: float
    metric_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestRunManifest:
    backtest_run_id: str
    target_source_ref: str
    market_data_ref: str
    initial_cash: Decimal
    execution_config: dict[str, Any]
    cost_config: dict[str, Any]
    status: BacktestRunStatus
    started_at: str
    finished_at: str
    config_hash: str
    code_version: str
    row_count_fill: int
    row_count_position: int
    row_count_nav: int
    row_count_metric: int
    universe_ref: str | None = None
    calendar_ref: str | None = None
    daily_status_ref: str | None = None
    coverage_report_ref: str | None = None


@dataclass(frozen=True)
class BacktestRunResult:
    manifest: BacktestRunManifest
    manifest_ref: DataRef
    fill_ref: DataRef
    position_ref: DataRef
    nav_ref: DataRef
    metric_ref: DataRef
    reused_existing: bool = False
