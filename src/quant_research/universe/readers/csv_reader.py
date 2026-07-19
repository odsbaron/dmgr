from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from quant_research.universe.contracts import UniverseSourceSpec
from quant_research.universe.readers.base import RawUniverseRow


class CSVUniverseReader:
    def read_rows(self, spec: UniverseSourceSpec) -> Iterator[RawUniverseRow]:
        with Path(spec.path).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader, start=1):
                yield RawUniverseRow(
                    source_row_id=str(index),
                    values={key: value for key, value in row.items()},
                )
