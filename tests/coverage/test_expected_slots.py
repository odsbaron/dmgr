from dataclasses import replace
from datetime import UTC, date, datetime, time

from quant_research.contracts.bar import AssetClass, Frequency
from quant_research.coverage import (
    CoveragePolicy,
    CoverageRunRequest,
    ExpectedSlotGenerator,
    TimestampConvention,
)
from quant_research.daily_status.contracts import (
    BarExpectation,
    DailyStatusRef,
    InstrumentDailyStatus,
    LocalTimeInterval,
    MarketState,
    ResolvedDailyStatus,
)
from quant_research.market_calendar.contracts import (
    CalendarDaySnapshot,
    CalendarRef,
    MarketSession,
    ResolvedMarketCalendar,
)
from quant_research.universe.contracts import (
    DailyUniverseMembership,
    ResolvedUniverse,
    UniverseRef,
)


TRADING_DATE = date(2026, 7, 7)


def request(
    *,
    freq: Frequency = Frequency.M1,
    convention: TimestampConvention = TimestampConvention.BAR_END,
) -> CoverageRunRequest:
    return CoverageRunRequest(
        coverage_run_id="coverage-run-1",
        market_data_ref="duckdb://curated_market_bar?snapshot_set_id=market-set",
        calendar_ref="duckdb://market_calendar_day?snapshot_set_id=calendar-set",
        universe_ref="duckdb://universe_member?snapshot_set_id=universe-set",
        daily_status_ref="duckdb://instrument_daily_status?snapshot_set_id=status-set",
        date_start=TRADING_DATE,
        date_end=TRADING_DATE,
        freq=freq,
        timestamp_convention=convention,
        policy=CoveragePolicy.STRICT,
    )


def calendar() -> ResolvedMarketCalendar:
    day = CalendarDaySnapshot(
        snapshot_id="calendar-day-1",
        calendar_id="xshg-xshe",
        calendar_version="v1",
        calendar_date=TRADING_DATE,
        is_trading_day=True,
        known_at=datetime(2026, 7, 7, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 6, tzinfo=UTC),
        definition_hash="sha256:calendar-definition",
        content_hash="sha256:calendar-day",
        source_ref="calendar.csv",
        source_file_hash="sha256:calendar-file",
        sessions=(
            MarketSession("morning", time(9, 30), time(11, 30)),
            MarketSession("afternoon", time(13), time(15)),
        ),
    )
    return ResolvedMarketCalendar(
        calendar_ref=CalendarRef("calendar-set"),
        calendar_id="xshg-xshe",
        calendar_version="v1",
        timezone="Asia/Shanghai",
        definition_hash="sha256:calendar-definition",
        snapshot_set_hash="sha256:calendar-set",
        days_by_date={TRADING_DATE: day},
    )


def universe(*symbols: str) -> ResolvedUniverse:
    return ResolvedUniverse(
        universe_ref=UniverseRef("universe-set"),
        universe_id="ashare",
        universe_version="v1",
        asset_class=AssetClass.EQUITY,
        calendar_id="xshg-xshe",
        definition_hash="sha256:universe-definition",
        snapshot_set_hash="sha256:universe-set",
        daily_memberships=(
            DailyUniverseMembership(TRADING_DATE, "universe-day-1", tuple(symbols)),
        ),
    )


def statuses(**values: BarExpectation) -> ResolvedDailyStatus:
    rows = {}
    for symbol, expectation in values.items():
        custom = (
            (LocalTimeInterval(time(10), time(10, 10)),)
            if expectation == BarExpectation.CUSTOM_INTERVALS
            else ()
        )
        rows[symbol] = InstrumentDailyStatus(
            instrument_id=symbol,
            market_state=(
                MarketState.SUSPENDED
                if expectation == BarExpectation.NO_BARS
                else MarketState.ACTIVE
            ),
            bar_expectation=expectation,
            custom_intervals=custom,
        )
    return ResolvedDailyStatus(
        status_ref=DailyStatusRef("status-set"),
        status_id="ashare-status",
        status_version="v1",
        asset_class=AssetClass.EQUITY,
        calendar_id="xshg-xshe",
        calendar_version="v1",
        timezone="Asia/Shanghai",
        definition_hash="sha256:status-definition",
        snapshot_set_hash="sha256:status-set",
        statuses_by_date={TRADING_DATE: rows},
    )


def test_split_sessions_generate_end_timestamp_slots_without_crossing_lunch():
    generation = ExpectedSlotGenerator().generate(
        request(),
        calendar(),
        universe("A"),
        statuses(A=BarExpectation.FULL_SESSION),
    )

    rendered = [slot.expected_at.isoformat() for slot in generation.slots]
    assert len(rendered) == 240
    assert rendered[0] == "2026-07-07T09:31:00+08:00"
    assert rendered[119] == "2026-07-07T11:30:00+08:00"
    assert rendered[120] == "2026-07-07T13:01:00+08:00"
    assert rendered[-1] == "2026-07-07T15:00:00+08:00"


def test_m5_start_timestamp_and_daily_identity_are_frequency_aware():
    minute = ExpectedSlotGenerator().generate(
        request(freq=Frequency.M5, convention=TimestampConvention.BAR_START),
        calendar(),
        universe("A"),
        statuses(A=BarExpectation.FULL_SESSION),
    )
    daily_request = request(freq=Frequency.D1)
    daily = ExpectedSlotGenerator().generate(
        daily_request,
        calendar(),
        universe("A"),
        statuses(A=BarExpectation.FULL_SESSION),
    )

    assert len(minute.slots) == 48
    assert minute.slots[0].expected_at.isoformat() == "2026-07-07T09:30:00+08:00"
    assert len(daily.slots) == 1
    assert daily.slots[0].comparison_key(Frequency.D1) == ("A", TRADING_DATE, None)


def test_no_bars_custom_intervals_and_unknown_semantics_are_explicit():
    generation = ExpectedSlotGenerator().generate(
        request(freq=Frequency.M5),
        calendar(),
        universe("ACTIVE", "SUSPENDED", "UNKNOWN"),
        statuses(
            ACTIVE=BarExpectation.CUSTOM_INTERVALS,
            SUSPENDED=BarExpectation.NO_BARS,
        ),
    )

    assert [(slot.symbol, slot.expected_at.time()) for slot in generation.slots] == [
        ("ACTIVE", time(10, 5)),
        ("ACTIVE", time(10, 10)),
    ]
    assert ("SUSPENDED", TRADING_DATE) in generation.resolved_symbol_dates
    assert ("UNKNOWN", TRADING_DATE) not in generation.resolved_symbol_dates
    assert [issue.issue_code for issue in generation.issues] == ["UNKNOWN_EXPECTATION"]


def test_request_rejects_invalid_range_and_ratio():
    valid = request()
    try:
        replace(valid, date_start=date(2026, 7, 8))
        raise AssertionError("reversed range must fail")
    except ValueError as exc:
        assert str(exc) == "date_start must be <= date_end"

    try:
        replace(valid, minimum_coverage_ratio=1.1)
        raise AssertionError("invalid ratio must fail")
    except ValueError as exc:
        assert "between 0 and 1" in str(exc)
