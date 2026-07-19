from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from quant_research import __version__
from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.coverage import (
    CoverageAnalyzer,
    CoverageConsumptionGate,
    CoverageGateError,
    CoverageIssue,
    CoverageIssueSeverity,
    CoveragePolicy,
    CoverageRunManifest,
    CoverageRunRequest,
    CoverageStoreError,
    ExpectedSlot,
    ExpectedSlotGeneration,
    LocalDuckDBCoverageStore,
    TimestampConvention,
)


DAY = date(2026, 7, 7)
ZONE = ZoneInfo("Asia/Shanghai")


def request(*, policy=CoveragePolicy.STRICT, run_id="coverage-run-1"):
    return CoverageRunRequest(
        coverage_run_id=run_id,
        market_data_ref="duckdb://curated_market_bar?snapshot_set_id=market-set",
        calendar_ref="duckdb://market_calendar_day?snapshot_set_id=calendar-set",
        universe_ref="duckdb://universe_member?snapshot_set_id=universe-set",
        daily_status_ref="duckdb://instrument_daily_status?snapshot_set_id=status-set",
        date_start=DAY,
        date_end=DAY,
        freq=Frequency.M1,
        timestamp_convention=TimestampConvention.BAR_END,
        policy=policy,
    )


def generation() -> ExpectedSlotGeneration:
    slots = tuple(
        ExpectedSlot(DAY, "A", datetime(2026, 7, 7, 9, minute, tzinfo=ZONE)) for minute in (31, 32)
    )
    return ExpectedSlotGeneration(
        slots=slots,
        scoped_symbol_dates=frozenset({("A", DAY), ("SUSPENDED", DAY)}),
        resolved_symbol_dates=frozenset({("A", DAY), ("SUSPENDED", DAY)}),
        issues=(),
    )


def bar(symbol: str, minute: int) -> BarRecord:
    end = datetime(2026, 7, 7, 9, minute, tzinfo=ZONE)
    return BarRecord(
        dataset_id="ashare-1m",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        trading_date=DAY,
        bar_start_time=end - timedelta(minutes=1),
        bar_end_time=end,
        open="10",
        high="10",
        low="10",
        close="10",
        volume="100",
        turnover="1000",
        adjustment=Adjustment.NONE,
        source="fixture",
        source_run_id="market-run",
        source_row_id=f"{symbol}-{minute}",
        raw_ref=None,
    )


def manifest_for(req, analysis):
    return CoverageRunManifest.from_analysis(
        req,
        analysis,
        input_hashes={
            "market_data": "sha256:market",
            "calendar": "sha256:calendar",
            "universe": "sha256:universe",
            "daily_status": "sha256:status",
        },
        started_at=datetime(2026, 7, 7, tzinfo=UTC),
        code_version=__version__,
    )


def test_complete_empty_and_missing_metrics_are_deterministic():
    analyzer = CoverageAnalyzer()
    complete = analyzer.analyze(request(), generation(), [bar("A", 31), bar("A", 32)])
    missing = analyzer.analyze(request(), generation(), [bar("A", 31)])

    assert complete.run_metric.expected_bar_count == 2
    assert complete.run_metric.actual_bar_count == 2
    assert complete.run_metric.coverage_ratio == 1.0
    assert complete.consumable is True
    suspended = [metric for metric in complete.metrics if metric.symbol == "SUSPENDED"][0]
    assert suspended.coverage_ratio == 1.0
    assert missing.run_metric.missing_bar_count == 1
    assert missing.run_metric.coverage_ratio == 0.5
    assert missing.consumable is False
    assert [issue.issue_code for issue in missing.issues] == ["MISSING_EXPECTED_SLOT"]


def test_warning_policy_records_missing_and_unexpected_but_remains_consumable():
    analysis = CoverageAnalyzer().analyze(
        request(policy=CoveragePolicy.WARNING),
        generation(),
        [bar("A", 31), bar("A", 33), bar("SUSPENDED", 31)],
    )

    assert analysis.run_metric.missing_bar_count == 1
    assert analysis.run_metric.unexpected_bar_count == 2
    assert analysis.consumable is True
    assert {issue.issue_code for issue in analysis.issues} == {
        "MISSING_EXPECTED_SLOT",
        "UNEXPECTED_ACTUAL_SLOT",
    }


def test_unknown_semantics_blocks_warning_policy():
    unknown = ExpectedSlotGeneration(
        slots=(),
        scoped_symbol_dates=frozenset({("A", DAY)}),
        resolved_symbol_dates=frozenset(),
        issues=(
            CoverageIssue(
                coverage_run_id="coverage-run-1",
                issue_code="UNKNOWN_EXPECTATION",
                severity=CoverageIssueSeverity.ERROR,
                message="unknown",
                trading_date=DAY,
                symbol="A",
            ),
        ),
    )

    analysis = CoverageAnalyzer().analyze(
        request(policy=CoveragePolicy.WARNING),
        unknown,
        [bar("A", 31)],
    )

    assert analysis.consumable is False
    assert analysis.run_metric.actual_bar_count == 1
    assert analysis.run_metric.unexpected_bar_count == 0


def test_store_commit_reads_assets_reuses_idempotently_and_gate_accepts(tmp_path):
    req = request()
    analysis = CoverageAnalyzer().analyze(req, generation(), [bar("A", 31), bar("A", 32)])
    manifest = manifest_for(req, analysis)
    store = LocalDuckDBCoverageStore(tmp_path / "research.duckdb")

    first = store.commit(manifest, analysis.metrics, analysis.issues)
    second = store.commit(manifest, analysis.metrics, analysis.issues)

    assert first.reused_existing is False
    assert second.reused_existing is True
    assert store.get_manifest(first.manifest_ref) == manifest
    assert store.read_metrics(first.metric_ref) == analysis.metrics
    assert store.read_issues(first.issue_ref) == analysis.issues
    CoverageConsumptionGate(store).assert_report_consumable(first.manifest_ref)


def test_store_rejects_conflict_and_gate_rejects_non_consumable(tmp_path):
    req = request()
    analysis = CoverageAnalyzer().analyze(req, generation(), [bar("A", 31)])
    manifest = manifest_for(req, analysis)
    store = LocalDuckDBCoverageStore(tmp_path / "research.duckdb")
    result = store.commit(manifest, analysis.metrics, analysis.issues)

    with pytest.raises(CoverageStoreError) as conflict:
        store.commit(replace(manifest, config_hash="sha256:different"), (), ())
    assert conflict.value.code == "COVERAGE_RUN_CONFLICT"

    with pytest.raises(CoverageGateError) as blocked:
        CoverageConsumptionGate(store).assert_report_consumable(result.manifest_ref)
    assert blocked.value.code == "COVERAGE_REPORT_NOT_CONSUMABLE"
