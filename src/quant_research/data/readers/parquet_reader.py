from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import polars as pl

from quant_research.contracts.source import SourceSpec
from quant_research.data.readers.base import RawKLineRow


class ParquetKLineReader:
    def read_rows(self, spec: SourceSpec) -> Iterator[RawKLineRow]:
        frame = pl.read_parquet(Path(spec.path))
        for index, row in enumerate(frame.iter_rows(named=True), start=1):
            yield RawKLineRow(
                source_row_id=str(index),
                values={key: _string_value(value) for key, value in row.items()},
            )


def _string_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)
