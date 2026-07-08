from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from quant_research.contracts.bar import BarRecord


_FACTOR_FRAME_COLUMNS = [
    "dataset_id",
    "symbol",
    "exchange",
    "asset_class",
    "freq",
    "trading_date",
    "as_of",
    "bar_start_time",
    "bar_end_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "adjustment",
    "source_run_id",
]


def bars_to_factor_frame(bars: Iterable[BarRecord]) -> pl.LazyFrame:
    rows = [_bar_to_row(bar) for bar in bars]
    if not rows:
        raise ValueError("bars must not be empty")
    return pl.DataFrame(rows).select(_FACTOR_FRAME_COLUMNS).lazy()


def _bar_to_row(bar: BarRecord) -> dict[str, object]:
    return {
        "dataset_id": bar.dataset_id,
        "symbol": bar.symbol,
        "exchange": bar.exchange,
        "asset_class": bar.asset_class.value,
        "freq": bar.freq.value,
        "trading_date": bar.trading_date.isoformat(),
        "as_of": bar.bar_end_time,
        "bar_start_time": bar.bar_start_time,
        "bar_end_time": bar.bar_end_time,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
        "turnover": float(bar.turnover) if bar.turnover is not None else None,
        "adjustment": bar.adjustment.value,
        "source_run_id": bar.source_run_id,
    }

