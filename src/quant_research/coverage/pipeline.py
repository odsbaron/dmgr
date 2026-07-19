from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from quant_research import __version__
from quant_research.coverage.analyzer import CoverageAnalyzer
from quant_research.coverage.contracts import (
    CoverageIssue,
    CoverageIssueSeverity,
    CoverageRunManifest,
    CoverageRunRequest,
    CoverageRunResult,
)
from quant_research.coverage.duckdb_store import LocalDuckDBCoverageStore
from quant_research.coverage.expected_slots import ExpectedSlotGenerator
from quant_research.daily_status.contracts import ResolvedDailyStatus
from quant_research.daily_status.resolver import DailyStatusResolver
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.partition_contracts import MarketDataRef, ResolvedMarketData
from quant_research.data.resolver import MarketDataResolver
from quant_research.market_calendar.contracts import ResolvedMarketCalendar
from quant_research.market_calendar.resolver import CalendarResolver
from quant_research.universe.contracts import ResolvedUniverse
from quant_research.universe.resolver import UniverseResolver


class CoveragePipelineError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class CoveragePipeline:
    data_store: LocalDuckDBStore
    coverage_store: LocalDuckDBCoverageStore
    market_data_resolver: MarketDataResolver
    calendar_resolver: CalendarResolver
    universe_resolver: UniverseResolver
    daily_status_resolver: DailyStatusResolver
    slot_generator: ExpectedSlotGenerator = field(default_factory=ExpectedSlotGenerator)
    analyzer: CoverageAnalyzer = field(default_factory=CoverageAnalyzer)

    def run(self, request: CoverageRunRequest) -> CoverageRunResult:
        started_at = datetime.now(UTC)
        try:
            market_data = self.market_data_resolver.resolve(request.market_data_ref)
            calendar = self.calendar_resolver.resolve(request.calendar_ref)
            universe = self.universe_resolver.resolve(request.universe_ref)
            daily_status = self.daily_status_resolver.resolve(request.daily_status_ref)
            self._validate_inputs(request, market_data, calendar, universe, daily_status)
            bars = self.data_store.read_bars(MarketDataRef.parse(request.market_data_ref).uri)
            generation = self.slot_generator.generate(
                request,
                calendar,
                universe,
                daily_status,
            )
            analysis = self.analyzer.analyze(request, generation, bars)
            manifest = CoverageRunManifest.from_analysis(
                request,
                analysis,
                input_hashes={
                    "market_data": market_data.snapshot_set_hash,
                    "calendar": calendar.snapshot_set_hash,
                    "universe": universe.snapshot_set_hash,
                    "daily_status": daily_status.snapshot_set_hash,
                },
                started_at=started_at,
                code_version=__version__,
            )
            return self.coverage_store.commit(manifest, analysis.metrics, analysis.issues)
        except Exception as exc:
            code = getattr(exc, "code", "COVERAGE_PIPELINE_FAILED")
            message = getattr(exc, "message", str(exc))
            manifest = CoverageRunManifest.failed(
                request,
                started_at=started_at,
                code_version=__version__,
                error_code=code,
                error_message=message,
            )
            issue = CoverageIssue(
                coverage_run_id=request.coverage_run_id,
                issue_code=code,
                severity=CoverageIssueSeverity.ERROR,
                message=message,
            )
            return self.coverage_store.commit(manifest, (), (issue,))

    @staticmethod
    def _validate_inputs(
        request: CoverageRunRequest,
        market_data: ResolvedMarketData,
        calendar: ResolvedMarketCalendar,
        universe: ResolvedUniverse,
        daily_status: ResolvedDailyStatus,
    ) -> None:
        if market_data.freq != request.freq:
            raise CoveragePipelineError(
                "COVERAGE_FREQUENCY_MISMATCH",
                "market-data frequency does not match coverage request",
            )
        calendar_ids = {
            market_data.calendar_id,
            calendar.calendar_id,
            universe.calendar_id,
            daily_status.calendar_id,
        }
        if len(calendar_ids) != 1:
            raise CoveragePipelineError(
                "COVERAGE_CALENDAR_MISMATCH",
                "coverage inputs do not share one calendar_id",
            )
        if daily_status.calendar_version != calendar.calendar_version:
            raise CoveragePipelineError(
                "COVERAGE_CALENDAR_VERSION_MISMATCH",
                "DailyStatus calendar version does not match Calendar",
            )
        if market_data.timezone != calendar.timezone or daily_status.timezone != calendar.timezone:
            raise CoveragePipelineError(
                "COVERAGE_TIMEZONE_MISMATCH",
                "coverage inputs do not share one timezone",
            )
        if not (market_data.asset_class == universe.asset_class == daily_status.asset_class):
            raise CoveragePipelineError(
                "COVERAGE_ASSET_CLASS_MISMATCH",
                "coverage inputs do not share one asset class",
            )

        requested_dates = set(_date_range(request.date_start, request.date_end))
        missing_calendar = requested_dates - set(calendar.calendar_dates)
        if missing_calendar:
            raise CoveragePipelineError(
                "COVERAGE_CALENDAR_DATE_NOT_COVERED",
                _render_missing("Calendar", missing_calendar),
            )
        trading_dates = {
            value for value in requested_dates if calendar.days_by_date[value].is_trading_day
        }
        missing_universe = trading_dates - set(universe.members_by_date)
        if missing_universe:
            raise CoveragePipelineError(
                "COVERAGE_UNIVERSE_DATE_NOT_COVERED",
                _render_missing("Universe", missing_universe),
            )
        missing_status = trading_dates - set(daily_status.trading_dates)
        if missing_status:
            raise CoveragePipelineError(
                "COVERAGE_STATUS_DATE_NOT_COVERED",
                _render_missing("DailyStatus", missing_status),
            )
        missing_market = trading_dates - set(market_data.trading_dates)
        if missing_market:
            raise CoveragePipelineError(
                "COVERAGE_MARKET_DATE_NOT_COVERED",
                _render_missing("MarketData", missing_market),
            )


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _render_missing(name: str, dates: set[date]) -> str:
    rendered = ", ".join(value.isoformat() for value in sorted(dates))
    return f"{name} snapshot set does not cover dates: {rendered}"
