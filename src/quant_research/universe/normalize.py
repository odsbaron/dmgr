from __future__ import annotations

import json
from datetime import date, datetime
from typing import Iterable

from quant_research.universe.contracts import UniverseMember, UniverseSourceSpec
from quant_research.universe.readers.base import RawUniverseRow


def normalize_universe_rows(
    rows: Iterable[RawUniverseRow],
    spec: UniverseSourceSpec,
    *,
    import_run_id: str,
) -> tuple[UniverseMember, ...]:
    return tuple(_normalize_row(row, spec, import_run_id=import_run_id) for row in rows)


def _normalize_row(
    row: RawUniverseRow,
    spec: UniverseSourceSpec,
    *,
    import_run_id: str,
) -> UniverseMember:
    mapped = {
        canonical: row.values.get(source_column)
        for canonical, source_column in spec.field_mapping.items()
    }
    return UniverseMember(
        instrument_id=_text(mapped.get("instrument_id")),
        weight=_optional_float(mapped.get("weight")),
        rank=_optional_int(mapped.get("rank")),
        inclusion_tags=_tags(mapped.get("inclusion_tags")),
        declared_trading_date=_optional_date(mapped.get("trading_date")),
        source_row_id=row.source_row_id,
        raw_ref=f"raw://{spec.source_id}/{import_run_id}/{row.source_row_id}",
    )


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _optional_float(value: object) -> float | None:
    text = _text(value)
    return float(text) if text else None


def _optional_int(value: object) -> int | None:
    text = _text(value)
    return int(text) if text else None


def _optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _tags(value: object) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, list | tuple | set):
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))
    text = str(value).strip()
    if text.startswith("["):
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            raise ValueError("inclusion_tags JSON must be a list")
        return tuple(sorted({str(item).strip() for item in parsed if str(item).strip()}))
    separator = "|" if "|" in text else ","
    return tuple(sorted({item.strip() for item in text.split(separator) if item.strip()}))
