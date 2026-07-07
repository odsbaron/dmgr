from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from quant_research.contracts.source import SourceSpec
from quant_research.data.readers.base import RawKLineRow


class CSVKLineReader:
    def read_rows(self, spec: SourceSpec) -> Iterator[RawKLineRow]:
        with Path(spec.path).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader, start=1):
                yield RawKLineRow(source_row_id=str(index), values={k: v for k, v in row.items()})

