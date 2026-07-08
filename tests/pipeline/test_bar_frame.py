from datetime import UTC, date, datetime, timedelta

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.pipeline.bar_frame import bars_to_factor_frame


def bar(symbol: str, close: str, index: int, *, freq: Frequency = Frequency.D1) -> BarRecord:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=freq,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=start,
        bar_end_time=start + timedelta(minutes=1 if freq == Frequency.M1 else 0),
        open="10.0",
        high="10.5",
        low="9.9",
        close=close,
        volume="1000",
        turnover="10000",
        adjustment=Adjustment.NONE,
        source="csv",
        source_run_id="import-run-1",
        source_row_id=f"row-{index}",
        raw_ref="fixture.csv",
    )


def test_bars_to_factor_frame_preserves_keys_and_casts_numeric_values():
    frame = bars_to_factor_frame(
        [
            bar("000001.SZ", "10.1", 0),
            bar("000001.SZ", "10.3", 1),
        ]
    ).collect()

    assert frame.columns == [
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
    assert frame["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert frame["freq"].to_list() == ["1d", "1d"]
    assert frame["close"].to_list() == [10.1, 10.3]
    assert frame["as_of"].to_list()[0].isoformat() == "2026-07-01T07:00:00+00:00"


def test_bars_to_factor_frame_uses_minute_bar_end_time_as_as_of():
    frame = bars_to_factor_frame([bar("000001.SZ", "10.1", 0, freq=Frequency.M1)]).collect()

    assert frame["freq"].to_list() == ["1m"]
    assert frame["as_of"].to_list()[0].isoformat() == "2026-07-01T07:01:00+00:00"


def test_bars_to_factor_frame_rejects_empty_input():
    with pytest.raises(ValueError, match="bars must not be empty"):
        bars_to_factor_frame([])
