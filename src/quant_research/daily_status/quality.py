from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_research.daily_status.contracts import (
    BarExpectation,
    DailyStatusDefinition,
    DailyStatusSourceSpec,
    InstrumentDailyStatus,
)
from quant_research.temporal_assets import canonical_hash, is_aware


class StatusQualitySeverity(StrEnum):
    ERROR = "ERROR"


@dataclass(frozen=True)
class StatusQualityIssue:
    issue_id: str
    import_run_id: str
    issue_code: str
    severity: StatusQualitySeverity
    message: str
    instrument_id: str | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None


@dataclass(frozen=True)
class StatusQualityReport:
    import_run_id: str
    issues: tuple[StatusQualityIssue, ...]

    @property
    def issue_count(self) -> int:
        return len(self.issues)

    @property
    def has_blocking_errors(self) -> bool:
        return bool(self.issues)


@dataclass(frozen=True)
class StatusQualityValidator:
    import_run_id: str

    def validate(
        self,
        definition: DailyStatusDefinition,
        spec: DailyStatusSourceSpec,
        statuses: tuple[InstrumentDailyStatus, ...],
    ) -> StatusQualityReport:
        issues: list[StatusQualityIssue] = []
        if definition.key != (spec.status_id, spec.status_version):
            issues.append(self._issue("DEFINITION_SOURCE_MISMATCH", "definition/source mismatch"))
        if not statuses:
            issues.append(self._issue("EMPTY_SNAPSHOT", "status snapshot is empty"))
        if not is_aware(spec.known_at) or not is_aware(spec.source_data_cutoff):
            issues.append(self._issue("NAIVE_POINT_IN_TIME", "point-in-time values must be aware"))
        elif spec.source_data_cutoff > spec.known_at:
            issues.append(self._issue("FUTURE_SOURCE_CUTOFF", "source cutoff must be <= known_at"))
        seen: set[str] = set()
        for status in statuses:
            if not status.instrument_id:
                issues.append(self._status_issue(status, "EMPTY_INSTRUMENT_ID", "instrument_id is empty"))
            elif status.instrument_id in seen:
                issues.append(self._status_issue(status, "DUPLICATE_STATUS", "duplicate instrument status"))
            seen.add(status.instrument_id)
            if status.declared_trading_date and status.declared_trading_date != spec.trading_date:
                issues.append(self._status_issue(status, "PARTITION_DATE_MISMATCH", "date mismatch"))
            if status.bar_expectation == BarExpectation.CUSTOM_INTERVALS:
                if not status.custom_intervals:
                    issues.append(self._status_issue(status, "MISSING_CUSTOM_INTERVALS", "custom intervals required"))
            elif status.custom_intervals:
                issues.append(self._status_issue(status, "UNEXPECTED_CUSTOM_INTERVALS", "intervals require CUSTOM_INTERVALS"))
            ordered = sorted(status.custom_intervals, key=lambda item: item.start_time)
            for interval in ordered:
                if interval.start_time >= interval.end_time:
                    issues.append(self._status_issue(status, "INVALID_INTERVAL", "interval start must precede end"))
            for previous, current in zip(ordered, ordered[1:], strict=False):
                if current.start_time < previous.end_time:
                    issues.append(self._status_issue(status, "OVERLAPPING_INTERVALS", "intervals overlap"))
        return StatusQualityReport(self.import_run_id, tuple(issues))

    def _status_issue(
        self,
        status: InstrumentDailyStatus,
        code: str,
        message: str,
    ) -> StatusQualityIssue:
        return self._issue(
            code, message, instrument_id=status.instrument_id or None,
            source_row_id=status.source_row_id, raw_ref=status.raw_ref,
        )

    def _issue(
        self,
        code: str,
        message: str,
        *,
        instrument_id: str | None = None,
        source_row_id: str | None = None,
        raw_ref: str | None = None,
    ) -> StatusQualityIssue:
        issue_id = canonical_hash(
            {
                "import_run_id": self.import_run_id,
                "code": code,
                "instrument_id": instrument_id,
                "source_row_id": source_row_id,
            }
        )
        return StatusQualityIssue(
            issue_id, self.import_run_id, code, StatusQualitySeverity.ERROR,
            message, instrument_id, source_row_id, raw_ref,
        )
