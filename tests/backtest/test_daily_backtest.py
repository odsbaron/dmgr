from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from quant_research.backtest import (
    BacktestConflictError,
    DailyBacktestPipeline,
    DailyBacktestRequest,
    DailyExecutionConfig,
    LocalDuckDBBacktestStore,
    ProportionalCostConfig,
    Side,
)
from quant_research.backtest.contracts import BacktestError
from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.signals import TargetWeight


START = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)


def bar(symbol: str, index: int, *, open_price: str, close_price: str) -> BarRecord:
    timestamp = START + timedelta(days=index)
    return BarRecord(
        dataset_id="daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=timestamp,
        bar_end_time=timestamp,
        open=open_price,
        high=max(open_price, close_price, key=Decimal),
        low=min(open_price, close_price, key=Decimal),
        close=close_price,
        volume="100000",
        turnover="1000000",
        adjustment=Adjustment.NONE,
        source="fixture",
        source_run_id="import-v1",
        source_row_id=f"{symbol}-{index}",
        raw_ref="fixture.csv",
    )


def target(symbol: str = "000001.SZ") -> TargetWeight:
    return TargetWeight(
        portfolio_run_id="portfolio-v1",
        dataset_id="daily",
        symbol=symbol,
        freq="1d",
        as_of=START,
        available_at=START + timedelta(minutes=1),
        target_weight=1.0,
        source_score_ref="duckdb://prediction_table?model_run_id=model-v1",
    )


def request(*, run_id: str = "backtest-v1", costs=None) -> DailyBacktestRequest:
    return DailyBacktestRequest(
        backtest_run_id=run_id,
        target_source_ref="duckdb://target_weight?portfolio_run_id=portfolio-v1",
        market_data_ref="duckdb://curated_market_bar?snapshot_set_id=market-v1",
        target_weights=(target(),),
        bars=(
            bar("000001.SZ", 0, open_price="10", close_price="10"),
            bar("000001.SZ", 1, open_price="10", close_price="11"),
            bar("000001.SZ", 2, open_price="11", close_price="12"),
        ),
        initial_cash=Decimal("1000"),
        costs=costs or ProportionalCostConfig(),
    )


def test_daily_pipeline_executes_next_day_open_and_balances_nav(tmp_path):
    store = LocalDuckDBBacktestStore(tmp_path / "research.duckdb")
    result = DailyBacktestPipeline(store).run(request())
    fills = store.read_fills("backtest-v1")
    nav = store.read_nav("backtest-v1")

    assert result.manifest.row_count_fill == 1
    assert fills[0].side == Side.BUY
    assert fills[0].trading_date == date(2026, 7, 2)
    assert fills[0].price == Decimal("10")
    assert all(fill.trading_date != date(2026, 7, 1) for fill in fills)
    assert all(snapshot.nav == snapshot.cash + snapshot.market_value for snapshot in nav)
    assert nav[-1].nav == Decimal("1200")


class RejectAllEligibility:
    def is_eligible(self, _bar, _side):
        return False


def test_ineligible_bar_produces_no_fill_and_preserves_cash(tmp_path):
    store = LocalDuckDBBacktestStore(tmp_path / "research.duckdb")
    DailyBacktestPipeline(store, eligibility=RejectAllEligibility()).run(request())

    assert store.read_fills("backtest-v1") == []
    assert store.read_nav("backtest-v1")[-1].nav == Decimal("1000")


def test_costs_reduce_nav_and_are_reported(tmp_path):
    zero_store = LocalDuckDBBacktestStore(tmp_path / "zero.duckdb")
    cost_store = LocalDuckDBBacktestStore(tmp_path / "cost.duckdb")
    DailyBacktestPipeline(zero_store).run(request(run_id="zero"))
    DailyBacktestPipeline(cost_store).run(
        request(
            run_id="cost",
            costs=ProportionalCostConfig(buy_rate=Decimal("0.01")),
        )
    )

    assert cost_store.read_nav("cost")[-1].nav < zero_store.read_nav("zero")[-1].nav
    metrics = {
        metric.metric_name: metric.metric_value for metric in cost_store.read_metrics("cost")
    }
    assert metrics["total_cost"] > 0
    assert metrics["trade_count"] == 1


def test_identical_run_is_reused_and_changed_config_conflicts(tmp_path):
    store = LocalDuckDBBacktestStore(tmp_path / "research.duckdb")
    pipeline = DailyBacktestPipeline(store)

    first = pipeline.run(request())
    second = pipeline.run(request())

    assert first.reused_existing is False
    assert second.reused_existing is True
    assert len(store.read_fills("backtest-v1")) == 1

    changed = replace(request(), initial_cash=Decimal("2000"))
    with pytest.raises(BacktestConflictError) as exc_info:
        pipeline.run(changed)
    assert exc_info.value.code == "BACKTEST_RUN_CONFLICT"


def test_target_is_retried_until_its_next_eligible_open(tmp_path):
    store = LocalDuckDBBacktestStore(tmp_path / "research.duckdb")
    delayed = replace(
        request(),
        bars=(
            bar("000001.SZ", 0, open_price="10", close_price="10"),
            bar("000002.SZ", 1, open_price="20", close_price="20"),
            bar("000001.SZ", 2, open_price="12", close_price="12"),
        ),
    )

    DailyBacktestPipeline(store).run(delayed)

    fills = store.read_fills("backtest-v1")
    assert len(fills) == 1
    assert fills[0].symbol == "000001.SZ"
    assert fills[0].trading_date == date(2026, 7, 3)
    assert fills[0].price == Decimal("12")


def test_latest_complete_snapshot_supersedes_older_snapshot_on_same_open(tmp_path):
    store = LocalDuckDBBacktestStore(tmp_path / "research.duckdb")
    later_as_of = START + timedelta(hours=12)
    later = replace(
        target("000002.SZ"),
        as_of=later_as_of,
        available_at=later_as_of + timedelta(minutes=1),
    )
    multi_snapshot = replace(
        request(),
        target_weights=(target(), later),
        bars=(
            bar("000001.SZ", 0, open_price="10", close_price="10"),
            bar("000002.SZ", 0, open_price="20", close_price="20"),
            bar("000001.SZ", 1, open_price="10", close_price="10"),
            bar("000002.SZ", 1, open_price="20", close_price="20"),
        ),
    )

    DailyBacktestPipeline(store).run(multi_snapshot)

    fills = store.read_fills("backtest-v1")
    assert [fill.symbol for fill in fills] == ["000002.SZ"]
    assert fills[0].rebalance_as_of == later_as_of


def test_changed_bar_content_or_eligibility_policy_conflicts(tmp_path):
    bar_store = LocalDuckDBBacktestStore(tmp_path / "bar.duckdb")
    DailyBacktestPipeline(bar_store).run(request())
    changed_bars = replace(
        request(),
        bars=(*request().bars[:-1], bar("000001.SZ", 2, open_price="11", close_price="13")),
    )

    with pytest.raises(BacktestConflictError):
        DailyBacktestPipeline(bar_store).run(changed_bars)

    policy_store = LocalDuckDBBacktestStore(tmp_path / "policy.duckdb")
    DailyBacktestPipeline(policy_store).run(request())
    with pytest.raises(BacktestConflictError):
        DailyBacktestPipeline(policy_store, eligibility=RejectAllEligibility()).run(request())


def test_request_rejects_cross_section_exposure_above_one():
    second = replace(target("000002.SZ"), target_weight=0.5)
    first = replace(target(), target_weight=0.6)

    with pytest.raises(BacktestError) as exc_info:
        replace(request(), target_weights=(first, second))

    assert exc_info.value.code == "TARGET_EXPOSURE_EXCEEDED"


@pytest.mark.parametrize(
    ("factory", "expected_code"),
    [
        (
            lambda: ProportionalCostConfig(buy_rate=Decimal("1")),
            "INVALID_COST_RATE",
        ),
        (
            lambda: DailyExecutionConfig(convention="SAME_DAY_CLOSE"),
            "UNSUPPORTED_EXECUTION_CONVENTION",
        ),
    ],
)
def test_execution_configuration_rejects_unsupported_values(factory, expected_code):
    with pytest.raises(BacktestError) as exc_info:
        factory()

    assert exc_info.value.code == expected_code


def test_request_rejects_ref_for_wrong_table():
    with pytest.raises(BacktestError) as exc_info:
        replace(request(), market_data_ref="duckdb://feature_table?factor_run_id=factor-v1")

    assert exc_info.value.code == "INVALID_REF_TABLE"
