from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_research.market_calendar.contracts import (
    CalendarSourceSpec,
    MarketCalendarDefinition,
    MarketSession,
    NormalizedCalendarDay,
)
from quant_research.temporal_assets import canonical_hash, is_aware


class CalendarQualitySeverity(StrEnum):
    ERROR = "ERROR"


@dataclass(frozen=True)
class CalendarQualityIssue:
    issue_id: str
    import_run_id: str
    issue_code: str
    severity: CalendarQualitySeverity
    message: str
    session_id: str | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None


@dataclass(frozen=True)
class CalendarQualityReport:
    import_run_id: str
    issues: tuple[CalendarQualityIssue, ...]

    @property
    def issue_count(self) -> int:
        return len(self.issues)

    @property
    def has_blocking_errors(self) -> bool:
        return bool(self.issues)


@dataclass(frozen=True)
class CalendarQualityValidator:
    import_run_id: str

    def validate(
        self,
        definition: MarketCalendarDefinition,
        spec: CalendarSourceSpec,
        day: NormalizedCalendarDay,
    ) -> CalendarQualityReport:
        issues: list[CalendarQualityIssue] = []
        if definition.key != (spec.calendar_id, spec.calendar_version):
            issues.append(self._issue("DEFINITION_SOURCE_MISMATCH", "definition/source mismatch"))
        if not is_aware(spec.known_at) or not is_aware(spec.source_data_cutoff):
            issues.append(self._issue("NAIVE_POINT_IN_TIME", "point-in-time values must be aware"))
        elif spec.source_data_cutoff > spec.known_at:
            issues.append(self._issue("FUTURE_SOURCE_CUTOFF", "source cutoff must be <= known_at"))
        if len(set(day.declared_states)) != 1:
            issues.append(self._issue("INCONSISTENT_DAY_STATE", "rows disagree on open/closed state"))
        if any(value is not None and value != spec.calendar_date for value in day.declared_dates):
            issues.append(self._issue("PARTITION_DATE_MISMATCH", "calendar date mismatch"))
        if day.is_trading_day and not day.sessions:
            issues.append(self._issue("OPEN_DAY_WITHOUT_SESSIONS", "open day requires sessions"))
        if not day.is_trading_day and day.sessions:
            issues.append(self._issue("CLOSED_DAY_WITH_SESSIONS", "closed day cannot have sessions"))

        seen: set[str] = set()
        ordered: list[MarketSession] = []
        for session in day.sessions:
            if not session.session_id:
                issues.append(self._session_issue(session, "EMPTY_SESSION_ID", "session_id is empty"))
            elif session.session_id in seen:
                issues.append(self._session_issue(session, "DUPLICATE_SESSION", "duplicate session_id"))
            seen.add(session.session_id)
            if session.start_time is None or session.end_time is None:
                issues.append(self._session_issue(session, "MISSING_SESSION_TIME", "session times required"))
                continue
            if session.start_time >= session.end_time:
                issues.append(self._session_issue(session, "INVALID_SESSION_RANGE", "start must precede end"))
            if (
                session.declared_calendar_date is not None
                and session.declared_calendar_date != spec.calendar_date
            ):
                issues.append(self._session_issue(session, "PARTITION_DATE_MISMATCH", "date mismatch"))
            ordered.append(session)
        ordered.sort(key=lambda item: item.start_time or item.end_time)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.end_time is not None and current.start_time is not None:
                if current.start_time < previous.end_time:
                    issues.append(self._session_issue(current, "OVERLAPPING_SESSIONS", "sessions overlap"))
        return CalendarQualityReport(self.import_run_id, tuple(issues))

    def _session_issue(
        self,
        session: MarketSession,
        code: str,
        message: str,
    ) -> CalendarQualityIssue:
        return self._issue(
            code,
            message,
            session_id=session.session_id or None,
            source_row_id=session.source_row_id,
            raw_ref=session.raw_ref,
        )

    def _issue(
        self,
        code: str,
        message: str,
        *,
        session_id: str | None = None,
        source_row_id: str | None = None,
        raw_ref: str | None = None,
    ) -> CalendarQualityIssue:
        issue_id = canonical_hash(
            {
                "import_run_id": self.import_run_id,
                "code": code,
                "session_id": session_id,
                "source_row_id": source_row_id,
            }
        )
        return CalendarQualityIssue(
            issue_id,
            self.import_run_id,
            code,
            CalendarQualitySeverity.ERROR,
            message,
            session_id,
            source_row_id,
            raw_ref,
        )
