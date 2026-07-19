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

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "asset_class": self.asset_class.value,
            "freq": self.freq.value,
            "trading_date": self.trading_date.isoformat(),
            "bar_start_time": self.bar_start_time.isoformat(),
            "bar_end_time": self.bar_end_time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "turnover": self.turnover,
            "adjustment": self.adjustment.value,
            "source": self.source,
            "source_run_id": self.source_run_id,
            "source_row_id": self.source_row_id,
            "raw_ref": self.raw_ref,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BarRecord":
        return cls(
            dataset_id=str(payload["dataset_id"]),
            symbol=str(payload["symbol"]),
            exchange=str(payload["exchange"]),
            asset_class=AssetClass(str(payload["asset_class"])),
            freq=Frequency(str(payload["freq"])),
            trading_date=date.fromisoformat(str(payload["trading_date"])),
            bar_start_time=datetime.fromisoformat(str(payload["bar_start_time"])),
            bar_end_time=datetime.fromisoformat(str(payload["bar_end_time"])),
            open=str(payload["open"]),
            high=str(payload["high"]),
            low=str(payload["low"]),
            close=str(payload["close"]),
            volume=str(payload["volume"]),
            turnover=(str(payload["turnover"]) if payload.get("turnover") is not None else None),
            adjustment=Adjustment(str(payload["adjustment"])),
            source=str(payload["source"]),
            source_run_id=str(payload["source_run_id"]),
            source_row_id=(
                str(payload["source_row_id"]) if payload.get("source_row_id") is not None else None
            ),
            raw_ref=str(payload["raw_ref"]) if payload.get("raw_ref") is not None else None,
        )
