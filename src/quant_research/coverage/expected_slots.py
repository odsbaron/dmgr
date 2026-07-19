from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from quant_research.contracts.bar import Frequency
from quant_research.coverage.contracts import (
    CoverageIssue,
    CoverageIssueSeverity,
    CoverageRunRequest,
    ExpectedSlot,
    TimestampConvention,
)
from quant_research.daily_status.contracts import (
    BarExpectation,
    InstrumentDailyStatus,
    LocalTimeInterval,
    ResolvedDailyStatus,
)
from quant_research.market_calendar.contracts import (
    CalendarDaySnapshot,
    MarketSession,
    ResolvedMarketCalendar,
)
from quant_research.universe.contracts import ResolvedUniverse


_FREQUENCY_MINUTES = {
    Frequency.M1: 1,
    Frequency.M5: 5,
    Frequency.M15: 15,
    Frequency.M30: 30,
    Frequency.M60: 60,
}


@dataclass(frozen=True)
class ExpectedSlotGeneration:
    slots: tuple[ExpectedSlot, ...]
    scoped_symbol_dates: frozenset[tuple[str, date]]
    resolved_symbol_dates: frozenset[tuple[str, date]]
    issues: tuple[CoverageIssue, ...]


class ExpectedSlotGenerator:
    def generate(
        self,
        request: CoverageRunRequest,
        calendar: ResolvedMarketCalendar,
        universe: ResolvedUniverse,
        daily_status: ResolvedDailyStatus,
    ) -> ExpectedSlotGeneration:
        slots: list[ExpectedSlot] = []
        issues: list[CoverageIssue] = []
        scoped: set[tuple[str, date]] = set()
        resolved: set[tuple[str, date]] = set()
        members_by_date = universe.members_by_date

        for trading_date in sorted(calendar.days_by_date):
            if not request.date_start <= trading_date <= request.date_end:
                continue
            calendar_day = calendar.days_by_date[trading_date]
            members = members_by_date.get(trading_date, frozenset())
            if not calendar_day.is_trading_day:
                for symbol in sorted(members):
                    scoped.add((symbol, trading_date))
                    resolved.add((symbol, trading_date))
                continue
            statuses = daily_status.statuses_by_date.get(trading_date, {})
            for symbol in sorted(members):
                key = (symbol, trading_date)
                scoped.add(key)
                status = statuses.get(symbol)
                if status is None or status.bar_expectation == BarExpectation.UNKNOWN:
                    issues.append(
                        CoverageIssue(
                            coverage_run_id=request.coverage_run_id,
                            issue_code="UNKNOWN_EXPECTATION",
                            severity=CoverageIssueSeverity.ERROR,
                            message="DailyStatus does not define expected bars for Universe member",
                            trading_date=trading_date,
                            symbol=symbol,
                        )
                    )
                    continue
                resolved.add(key)
                slots.extend(
                    self._slots_for_status(
                        request,
                        calendar,
                        calendar_day,
                        trading_date,
                        symbol,
                        status,
                        issues,
                    )
                )

        return ExpectedSlotGeneration(
            slots=tuple(sorted(slots)),
            scoped_symbol_dates=frozenset(scoped),
            resolved_symbol_dates=frozenset(resolved),
            issues=tuple(sorted(issues, key=lambda item: item.issue_id)),
        )

    def _slots_for_status(
        self,
        request: CoverageRunRequest,
        calendar: ResolvedMarketCalendar,
        calendar_day: CalendarDaySnapshot,
        trading_date: date,
        symbol: str,
        status: InstrumentDailyStatus,
        issues: list[CoverageIssue],
    ) -> list[ExpectedSlot]:
        if status.bar_expectation == BarExpectation.NO_BARS:
            return []
        if status.bar_expectation == BarExpectation.CUSTOM_INTERVALS:
            intervals = status.custom_intervals
        else:
            intervals = tuple(
                LocalTimeInterval(session.start_time, session.end_time)
                for session in calendar_day.sessions
                if session.start_time is not None and session.end_time is not None
            )
            if len(intervals) != len(calendar_day.sessions):
                issues.append(
                    CoverageIssue(
                        coverage_run_id=request.coverage_run_id,
                        issue_code="INVALID_CALENDAR_SESSION",
                        severity=CoverageIssueSeverity.ERROR,
                        message="Intraday coverage requires start and end times for every session",
                        trading_date=trading_date,
                        symbol=symbol,
                    )
                )
                return []

        if request.freq == Frequency.D1:
            expected_at = self._daily_expected_at(
                trading_date,
                calendar.timezone,
                calendar_day.sessions,
                request.timestamp_convention,
            )
            return [ExpectedSlot(trading_date, symbol, expected_at)]

        zone = ZoneInfo(calendar.timezone)
        result: list[ExpectedSlot] = []
        for interval in intervals:
            if interval.start_time >= interval.end_time:
                issues.append(
                    CoverageIssue(
                        coverage_run_id=request.coverage_run_id,
                        issue_code="INVALID_EXPECTED_INTERVAL",
                        severity=CoverageIssueSeverity.ERROR,
                        message="Expected interval start must be before end",
                        trading_date=trading_date,
                        symbol=symbol,
                    )
                )
                continue
            start = datetime.combine(trading_date, interval.start_time, tzinfo=zone)
            end = datetime.combine(trading_date, interval.end_time, tzinfo=zone)
            result.extend(self._split_interval(request, symbol, trading_date, start, end))
        return result

    def _split_interval(
        self,
        request: CoverageRunRequest,
        symbol: str,
        trading_date: date,
        start: datetime,
        end: datetime,
    ) -> list[ExpectedSlot]:
        step = timedelta(minutes=_FREQUENCY_MINUTES[request.freq])
        cursor = start
        result = []
        while cursor + step <= end:
            expected_at = (
                cursor
                if request.timestamp_convention == TimestampConvention.BAR_START
                else cursor + step
            )
            result.append(ExpectedSlot(trading_date, symbol, expected_at))
            cursor += step
        return result

    @staticmethod
    def _daily_expected_at(
        trading_date: date,
        timezone: str,
        sessions: tuple[MarketSession, ...],
        convention: TimestampConvention,
    ) -> datetime | None:
        timed = [session for session in sessions if session.start_time and session.end_time]
        if not timed:
            return None
        zone = ZoneInfo(timezone)
        value = (
            timed[0].start_time
            if convention == TimestampConvention.BAR_START
            else timed[-1].end_time
        )
        return datetime.combine(trading_date, value, tzinfo=zone)
