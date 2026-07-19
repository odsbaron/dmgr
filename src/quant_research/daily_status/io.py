from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterator, Mapping, Protocol

import polars as pl

from quant_research.daily_status.contracts import (
    BarExpectation,
    DailyStatusSourceSpec,
    InstrumentDailyStatus,
    LocalTimeInterval,
    MarketState,
)


@dataclass(frozen=True)
class RawDailyStatusRow:
    source_row_id: str
    values: Mapping[str, object]


class DailyStatusReader(Protocol):
    def read_rows(self, spec: DailyStatusSourceSpec) -> Iterator[RawDailyStatusRow]: ...


class CSVStatusReader:
    def read_rows(self, spec: DailyStatusSourceSpec) -> Iterator[RawDailyStatusRow]:
        with Path(spec.path).open("r", encoding="utf-8", newline="") as handle:
            for index, row in enumerate(csv.DictReader(handle), start=1):
                yield RawDailyStatusRow(str(index), row)


class ParquetStatusReader:
    def read_rows(self, spec: DailyStatusSourceSpec) -> Iterator[RawDailyStatusRow]:
        for index, row in enumerate(pl.read_parquet(spec.path).to_dicts(), start=1):
            yield RawDailyStatusRow(str(index), row)


def normalize_status_rows(
    rows: list[RawDailyStatusRow],
    spec: DailyStatusSourceSpec,
    *,
    import_run_id: str,
) -> tuple[InstrumentDailyStatus, ...]:
    return tuple(_normalize(row, spec, import_run_id) for row in rows)


def _normalize(
    row: RawDailyStatusRow,
    spec: DailyStatusSourceSpec,
    import_run_id: str,
) -> InstrumentDailyStatus:
    mapped = {
        canonical: row.values.get(source)
        for canonical, source in spec.field_mapping.items()
    }
    return InstrumentDailyStatus(
        instrument_id=_text(mapped.get("instrument_id")),
        market_state=MarketState(_text(mapped.get("market_state")).upper()),
        bar_expectation=BarExpectation(_text(mapped.get("bar_expectation")).upper()),
        custom_intervals=_intervals(mapped.get("custom_intervals")),
        declared_trading_date=_optional_date(mapped.get("trading_date")),
        source_row_id=row.source_row_id,
        raw_ref=f"raw://{spec.source_id}/{import_run_id}/{row.source_row_id}",
    )


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _intervals(value: object) -> tuple[LocalTimeInterval, ...]:
    if value is None or value == "":
        return ()
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        if text.startswith("["):
            parsed = json.loads(text)
        else:
            parsed = []
            for item in text.split("|"):
                start, separator, end = item.partition("-")
                if not separator:
                    raise ValueError("custom interval must use start-end")
                parsed.append({"start_time": start, "end_time": end})
    if not isinstance(parsed, list | tuple):
        raise ValueError("custom_intervals must be a list")
    result = []
    for item in parsed:
        if not isinstance(item, Mapping):
            raise ValueError("custom interval must be an object")
        start = item.get("start_time", item.get("start"))
        end = item.get("end_time", item.get("end"))
        result.append(LocalTimeInterval(time.fromisoformat(str(start)), time.fromisoformat(str(end))))
    return tuple(result)
