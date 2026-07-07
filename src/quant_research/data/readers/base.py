from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Mapping, Protocol

from quant_research.contracts.source import SourceSpec


@dataclass(frozen=True)
class RawKLineRow:
    source_row_id: str
    values: Mapping[str, str]

    def with_values(self, updates: Mapping[str, str]) -> "RawKLineRow":
        merged = dict(self.values)
        merged.update(updates)
        return replace(self, values=merged)


class KLineReader(Protocol):
    def read_rows(self, spec: SourceSpec) -> Iterable[RawKLineRow]:
        ...

