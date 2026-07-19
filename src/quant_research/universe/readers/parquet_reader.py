from __future__ import annotations

from typing import Iterator

import polars as pl

from quant_research.universe.contracts import UniverseSourceSpec
from quant_research.universe.readers.base import RawUniverseRow


class ParquetUniverseReader:
    def read_rows(self, spec: UniverseSourceSpec) -> Iterator[RawUniverseRow]:
        for index, row in enumerate(pl.read_parquet(spec.path).to_dicts(), start=1):
            yield RawUniverseRow(source_row_id=str(index), values=row)
