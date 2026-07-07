from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from quant_research.contracts.bar import AssetClass, BarRecord, Frequency
from quant_research.contracts.source import SourceSpec
from quant_research.data.readers.base import RawKLineRow


_FREQ_TO_DELTA = {
    Frequency.M1: timedelta(minutes=1),
    Frequency.M5: timedelta(minutes=5),
    Frequency.M15: timedelta(minutes=15),
    Frequency.M30: timedelta(minutes=30),
    Frequency.M60: timedelta(minutes=60),
}


@dataclass(frozen=True)
class BarNormalizer:
    import_run_id: str

    def normalize(self, row: RawKLineRow, spec: SourceSpec) -> BarRecord:
        mapped = self._canonical_values(row, spec)
        symbol = spec.symbol_mapping.get(mapped["symbol"], mapped["symbol"]).strip().upper()
        exchange = mapped["exchange"].strip().upper()
        start, end, trading_date = self._bar_window(mapped, spec)
        return BarRecord(
            dataset_id=spec.dataset_id,
            symbol=symbol,
            exchange=exchange,
            asset_class=self._asset_class(exchange),
            freq=spec.freq,
            trading_date=trading_date,
            bar_start_time=start,
            bar_end_time=end,
            open=mapped["open"].strip(),
            high=mapped["high"].strip(),
            low=mapped["low"].strip(),
            close=mapped["close"].strip(),
            volume=mapped["volume"].strip(),
            turnover=(mapped.get("turnover") or "").strip() or None,
            adjustment=spec.adjustment,
            source=spec.source_id,
            source_run_id=self.import_run_id,
            source_row_id=row.source_row_id,
            raw_ref=f"raw://{spec.source_id}/{self.import_run_id}/{row.source_row_id}",
        )

    def _canonical_values(self, row: RawKLineRow, spec: SourceSpec) -> dict[str, str]:
        result: dict[str, str] = {}
        for canonical, source_column in spec.field_mapping.items():
            result[canonical] = row.values.get(source_column, "")
        return result

    def _bar_window(
        self,
        values: dict[str, str],
        spec: SourceSpec,
    ) -> tuple[datetime, datetime, date]:
        zone = ZoneInfo(spec.timezone)
        if spec.freq == Frequency.D1:
            trading_date = date.fromisoformat(values["date"])
            start_local = datetime.combine(trading_date, time(9, 30), tzinfo=zone)
            end_local = datetime.combine(trading_date, time(15, 0), tzinfo=zone)
            return start_local.astimezone(UTC), end_local.astimezone(UTC), trading_date

        raw_timestamp = values.get("datetime") or values.get("bar_start_time")
        start_local = datetime.fromisoformat(raw_timestamp)
        if start_local.tzinfo is None:
            start_local = start_local.replace(tzinfo=zone)
        start = start_local.astimezone(UTC)
        end = start + _FREQ_TO_DELTA[spec.freq]
        return start, end, start_local.date()

    def _asset_class(self, exchange: str) -> AssetClass:
        if exchange in {"CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"}:
            return AssetClass.FUTURE
        return AssetClass.EQUITY

