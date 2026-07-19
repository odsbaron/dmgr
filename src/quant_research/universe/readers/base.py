from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Mapping, Protocol

from quant_research.universe.contracts import UniverseSourceSpec


@dataclass(frozen=True)
class RawUniverseRow:
    source_row_id: str
    values: Mapping[str, object]


class UniverseReader(Protocol):
    def read_rows(self, spec: UniverseSourceSpec) -> Iterator[RawUniverseRow]:
        ...
