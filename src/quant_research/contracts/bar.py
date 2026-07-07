from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class Frequency(StrEnum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    M60 = "60m"
    D1 = "1d"


class AssetClass(StrEnum):
    EQUITY = "EQUITY"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    FUND = "FUND"
    CRYPTO = "CRYPTO"


class Adjustment(StrEnum):
    NONE = "NONE"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


@dataclass(frozen=True)
class BarRecord:
    dataset_id: str
    symbol: str
    exchange: str
    asset_class: AssetClass
    freq: Frequency
    trading_date: date
    bar_start_time: datetime
    bar_end_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: str
    turnover: str | None
    adjustment: Adjustment
    source: str
    source_run_id: str
    source_row_id: str | None
    raw_ref: str | None

