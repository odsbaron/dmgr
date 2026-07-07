from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from quant_research.contracts.bar import Frequency


class Severity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class QualityIssue:
    issue_id: str
    import_run_id: str
    dataset_id: str
    symbol: str | None
    freq: Frequency | None
    trading_date: date | None
    bar_start_time: datetime | None
    issue_code: str
    severity: Severity
    message: str
    raw_ref: str | None

    @property
    def is_blocking(self) -> bool:
        return self.severity == Severity.ERROR


@dataclass(frozen=True)
class QualityReport:
    import_run_id: str
    issues: tuple[QualityIssue, ...]

    @property
    def issue_count(self) -> int:
        return len(self.issues)

    @property
    def has_blocking_errors(self) -> bool:
        return any(issue.is_blocking for issue in self.issues)

