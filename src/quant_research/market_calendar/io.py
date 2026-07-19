from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterator, Mapping, Protocol

import polars as pl

from quant_research.market_calendar.contracts import (
    CalendarSourceSpec,
    MarketSession,
    NormalizedCalendarDay,
)


@dataclass(frozen=True)
class RawCalendarRow:
    source_row_id: str
    values: Mapping[str, object]


class CalendarReader(Protocol):
    def read_rows(self, spec: CalendarSourceSpec) -> Iterator[RawCalendarRow]: ...


class CSVCalendarReader:
    def read_rows(self, spec: CalendarSourceSpec) -> Iterator[RawCalendarRow]:
        with Path(spec.path).open("r", encoding="utf-8", newline="") as handle:
            for index, row in enumerate(csv.DictReader(handle), start=1):
                yield RawCalendarRow(str(index), row)


class ParquetCalendarReader:
    def read_rows(self, spec: CalendarSourceSpec) -> Iterator[RawCalendarRow]:
        for index, row in enumerate(pl.read_parquet(spec.path).to_dicts(), start=1):
            yield RawCalendarRow(str(index), row)


def normalize_calendar_rows(
    rows: list[RawCalendarRow],
    spec: CalendarSourceSpec,
    *,
    import_run_id: str,
) -> NormalizedCalendarDay:
    if not rows:
        raise ValueError("calendar source must contain at least one row")
    states: list[bool] = []
    dates: list[date | None] = []
    sessions: list[MarketSession] = []
    for row in rows:
        mapped = {
            canonical: row.values.get(source)
            for canonical, source in spec.field_mapping.items()
        }
        state = _bool(mapped.get("is_trading_day"))
        states.append(state)
        declared_date = _optional_date(mapped.get("calendar_date"))
        dates.append(declared_date)
        start = _optional_time(mapped.get("session_start"))
        end = _optional_time(mapped.get("session_end"))
        session_id = _text(mapped.get("session_id"))
        if session_id or start is not None or end is not None:
            sessions.append(
                MarketSession(
                    session_id=session_id,
                    start_time=start,
                    end_time=end,
                    session_kind=_text(mapped.get("session_kind")) or "REGULAR",
                    declared_calendar_date=declared_date,
                    source_row_id=row.source_row_id,
                    raw_ref=f"raw://{spec.source_id}/{import_run_id}/{row.source_row_id}",
                )
            )
    return NormalizedCalendarDay(
        states[0],
        tuple(states),
        tuple(sessions),
        len(rows),
        tuple(dates),
    )


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _text(value).lower()
    if normalized in {"1", "true", "yes", "y", "open"}:
        return True
    if normalized in {"0", "false", "no", "n", "closed"}:
        return False
    raise ValueError(f"invalid is_trading_day value: {value}")


def _optional_time(value: object) -> time | None:
    text = _text(value)
    if not text:
        return None
    parsed = time.fromisoformat(text)
    if parsed.tzinfo is not None:
        raise ValueError("session times must be local wall-clock times")
    return parsed


def _optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())
