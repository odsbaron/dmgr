import csv
import json
from datetime import UTC, date, datetime, time

from quant_research.contracts.bar import Adjustment, AssetClass, Frequency
from quant_research.contracts.source import BarTimestampConvention, SourceType
from quant_research.coverage import (
    CoveragePipeline,
    CoveragePolicy,
    CoverageRunRequest,
    CoverageRunStatus,
    LocalDuckDBCoverageStore,
    TimestampConvention,
)
from quant_research.daily_status import (
    DailyStatusDefinition,
    DailyStatusIngestionService,
    DailyStatusResolver,
    DailyStatusSourceSpec,
    LocalDuckDBDailyStatusStore,
    StatusSourceType,
)
from quant_research.data import (
    ImmutableMarketDataIngestionService,
    LocalDuckDBStore,
    MarketDataResolver,
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)
from quant_research.market_calendar import (
    CalendarIngestionService,
    CalendarResolver,
    CalendarSourceSpec,
    CalendarSourceType,
    LocalDuckDBCalendarStore,
    MarketCalendarDefinition,
)
from quant_research.universe import (
    LocalDuckDBUniverseStore,
    UniverseConstructionMode,
    UniverseDefinition,
    UniverseIngestionService,
    UniverseResolver,
    UniverseSourceSpec,
    UniverseSourceType,
)


DAY = date(2026, 7, 7)


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def seed_immutable_assets(tmp_path):
    db_path = tmp_path / "coverage.duckdb"
    data_store = LocalDuckDBStore(db_path)
    market_path = tmp_path / "market.csv"
    write_csv(
        market_path,
        [
            {
                "symbol": "A",
                "exchange": "XSHE",
                "datetime": "2026-07-07T09:30:00+08:00",
                "open": "10",
                "high": "10",
                "low": "10",
                "close": "10",
                "volume": "100",
                "turnover": "1000",
            },
            {
                "symbol": "A",
                "exchange": "XSHE",
                "datetime": "2026-07-07T09:31:00+08:00",
                "open": "10",
                "high": "10",
                "low": "10",
                "close": "10",
                "volume": "100",
                "turnover": "1000",
            },
        ],
    )
    market_definition = MarketDatasetDefinition(
        dataset_id="ashare-1m",
        version="v1",
        name="A-share minute fixture",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        adjustment=Adjustment.NONE,
        calendar_id="xshg-xshe",
        timezone="Asia/Shanghai",
        bar_timestamp_convention=BarTimestampConvention.START_TIME,
    )
    ImmutableMarketDataIngestionService(
        data_store,
        run_id_factory=lambda: "market-run-1",
    ).ingest(
        market_definition,
        MarketDataSourceSpec(
            source_id="market-source",
            dataset_id="ashare-1m",
            dataset_version="v1",
            source_type=SourceType.CSV,
            path=str(market_path),
            trading_date=DAY,
            known_at=datetime(2026, 7, 7, 8, tzinfo=UTC),
            source_data_cutoff=datetime(2026, 7, 7, 7, tzinfo=UTC),
            field_mapping={
                "symbol": "symbol",
                "exchange": "exchange",
                "datetime": "datetime",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "turnover": "turnover",
            },
        ),
    )
    market_ref = data_store.create_market_data_snapshot_set(
        dataset_id="ashare-1m",
        dataset_version="v1",
        trading_dates=(DAY,),
    ).ref

    calendar_path = tmp_path / "calendar.csv"
    write_csv(
        calendar_path,
        [
            {
                "calendar_date": DAY.isoformat(),
                "is_trading_day": "true",
                "session_id": "morning",
                "session_start": "09:30:00",
                "session_end": "11:30:00",
                "session_kind": "REGULAR",
            },
            {
                "calendar_date": DAY.isoformat(),
                "is_trading_day": "true",
                "session_id": "afternoon",
                "session_start": "13:00:00",
                "session_end": "15:00:00",
                "session_kind": "REGULAR",
            },
        ],
    )
    calendar_store = LocalDuckDBCalendarStore(db_path)
    CalendarIngestionService(
        calendar_store,
        run_id_factory=lambda: "calendar-run-1",
    ).ingest(
        MarketCalendarDefinition(
            calendar_id="xshg-xshe",
            version="v1",
            name="A-share calendar",
            timezone="Asia/Shanghai",
        ),
        CalendarSourceSpec(
            source_id="calendar-source",
            calendar_id="xshg-xshe",
            calendar_version="v1",
            source_type=CalendarSourceType.CSV,
            path=str(calendar_path),
            calendar_date=DAY,
            known_at=datetime(2026, 7, 7, tzinfo=UTC),
            source_data_cutoff=datetime(2026, 7, 6, 23, tzinfo=UTC),
            field_mapping={
                "calendar_date": "calendar_date",
                "is_trading_day": "is_trading_day",
                "session_id": "session_id",
                "session_start": "session_start",
                "session_end": "session_end",
                "session_kind": "session_kind",
            },
        ),
    )
    calendar_ref = calendar_store.create_snapshot_set(
        calendar_id="xshg-xshe",
        calendar_version="v1",
        calendar_dates=(DAY,),
    ).ref

    universe_path = tmp_path / "universe.csv"
    write_csv(
        universe_path,
        [
            {"trading_date": DAY.isoformat(), "instrument_id": "A"},
            {"trading_date": DAY.isoformat(), "instrument_id": "SUSPENDED"},
        ],
    )
    universe_store = LocalDuckDBUniverseStore(db_path)
    UniverseIngestionService(
        universe_store,
        run_id_factory=lambda: "universe-run-1",
    ).ingest(
        UniverseDefinition(
            universe_id="ashare",
            version="v1",
            name="A-share fixture",
            asset_class=AssetClass.EQUITY,
            calendar_id="xshg-xshe",
            timezone="Asia/Shanghai",
            selection_cutoff_time=time(9, 30),
            construction_mode=UniverseConstructionMode.IMPORTED_SNAPSHOT,
        ),
        UniverseSourceSpec(
            source_id="universe-source",
            universe_id="ashare",
            universe_version="v1",
            source_type=UniverseSourceType.CSV,
            path=str(universe_path),
            trading_date=DAY,
            known_at=datetime(2026, 7, 7, 1, tzinfo=UTC),
            source_data_cutoff=datetime(2026, 7, 6, 23, tzinfo=UTC),
            field_mapping={
                "trading_date": "trading_date",
                "instrument_id": "instrument_id",
            },
        ),
    )
    universe_ref = universe_store.create_snapshot_set(
        universe_id="ashare",
        universe_version="v1",
        trading_dates=(DAY,),
    ).ref

    status_path = tmp_path / "status.csv"
    write_csv(
        status_path,
        [
            {
                "trading_date": DAY.isoformat(),
                "instrument_id": "A",
                "market_state": "ACTIVE",
                "bar_expectation": "CUSTOM_INTERVALS",
                "custom_intervals": json.dumps(
                    [{"start_time": "09:30:00", "end_time": "09:32:00"}]
                ),
            },
            {
                "trading_date": DAY.isoformat(),
                "instrument_id": "SUSPENDED",
                "market_state": "SUSPENDED",
                "bar_expectation": "NO_BARS",
                "custom_intervals": "",
            },
        ],
    )
    status_store = LocalDuckDBDailyStatusStore(db_path)
    DailyStatusIngestionService(
        status_store,
        run_id_factory=lambda: "status-run-1",
    ).ingest(
        DailyStatusDefinition(
            status_id="ashare-status",
            version="v1",
            name="A-share status",
            asset_class=AssetClass.EQUITY,
            calendar_id="xshg-xshe",
            calendar_version="v1",
            timezone="Asia/Shanghai",
        ),
        DailyStatusSourceSpec(
            source_id="status-source",
            status_id="ashare-status",
            status_version="v1",
            source_type=StatusSourceType.CSV,
            path=str(status_path),
            trading_date=DAY,
            known_at=datetime(2026, 7, 7, 1, tzinfo=UTC),
            source_data_cutoff=datetime(2026, 7, 7, tzinfo=UTC),
            field_mapping={
                "trading_date": "trading_date",
                "instrument_id": "instrument_id",
                "market_state": "market_state",
                "bar_expectation": "bar_expectation",
                "custom_intervals": "custom_intervals",
            },
        ),
    )
    status_ref = status_store.create_snapshot_set(
        status_id="ashare-status",
        status_version="v1",
        trading_dates=(DAY,),
    ).ref

    coverage_store = LocalDuckDBCoverageStore(db_path)
    pipeline = CoveragePipeline(
        data_store=data_store,
        coverage_store=coverage_store,
        market_data_resolver=MarketDataResolver(data_store),
        calendar_resolver=CalendarResolver(calendar_store),
        universe_resolver=UniverseResolver(universe_store),
        daily_status_resolver=DailyStatusResolver(status_store),
    )
    request = CoverageRunRequest(
        coverage_run_id="coverage-run-1",
        market_data_ref=market_ref.uri,
        calendar_ref=calendar_ref.uri,
        universe_ref=universe_ref.uri,
        daily_status_ref=status_ref.uri,
        date_start=DAY,
        date_end=DAY,
        freq=Frequency.M1,
        timestamp_convention=TimestampConvention.BAR_END,
        policy=CoveragePolicy.STRICT,
    )
    return pipeline, coverage_store, request


def test_coverage_pipeline_commits_complete_report_from_immutable_assets(tmp_path):
    pipeline, store, request = seed_immutable_assets(tmp_path)

    result = pipeline.run(request)
    manifest = store.get_manifest(result.manifest_ref)

    assert result.status == CoverageRunStatus.COMMITTED
    assert result.consumable is True
    assert result.expected_bar_count == 2
    assert result.actual_bar_count == 2
    assert result.matched_bar_count == 2
    assert result.coverage_ratio == 1.0
    assert manifest is not None
    assert manifest.market_data_hash.startswith("sha256:")
    assert manifest.calendar_hash.startswith("sha256:")
    assert manifest.universe_hash.startswith("sha256:")
    assert manifest.daily_status_hash.startswith("sha256:")
    assert store.read_issues(result.issue_ref) == ()


def test_coverage_pipeline_persists_structured_compatibility_failure(tmp_path):
    pipeline, store, request = seed_immutable_assets(tmp_path)

    result = pipeline.run(
        CoverageRunRequest(
            **{
                **request.__dict__,
                "coverage_run_id": "coverage-run-failed",
                "freq": Frequency.M5,
            }
        )
    )

    assert result.status == CoverageRunStatus.FAILED
    assert result.consumable is False
    assert result.error_code == "COVERAGE_FREQUENCY_MISMATCH"
    assert store.get_manifest(result.manifest_ref).status == CoverageRunStatus.FAILED
    assert [issue.issue_code for issue in store.read_issues(result.issue_ref)] == [
        "COVERAGE_FREQUENCY_MISMATCH"
    ]
