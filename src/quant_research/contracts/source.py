from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Mapping

from quant_research.contracts.bar import Adjustment, Frequency


class SourceType(StrEnum):
    CSV = "CSV"
    PARQUET = "PARQUET"
    DUCKDB_TABLE = "DUCKDB_TABLE"


class BarTimestampConvention(StrEnum):
    START_TIME = "START_TIME"
    END_TIME = "END_TIME"


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    dataset_id: str
    source_type: SourceType
    path: str
    freq: Frequency
    timezone: str
    adjustment: Adjustment
    field_mapping: Mapping[str, str]
    symbol_mapping: Mapping[str, str] = field(default_factory=dict)
    calendar_id: str = "default"
    strict_mode: bool = True
    repair_mode: bool = False
    bar_timestamp_convention: BarTimestampConvention = BarTimestampConvention.START_TIME
