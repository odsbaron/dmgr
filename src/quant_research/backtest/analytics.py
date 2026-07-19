from __future__ import annotations

from decimal import Decimal

from quant_research.backtest.contracts import BacktestMetric, Fill, NavSnapshot


def analyze_backtest(
    backtest_run_id: str,
    nav_snapshots: list[NavSnapshot],
    fills: list[Fill],
    *,
    initial_cash: Decimal,
) -> list[BacktestMetric]:
    if not nav_snapshots:
        return []
    ordered = sorted(nav_snapshots, key=lambda item: (item.trading_date, item.as_of))
    first_nav = initial_cash
    last_nav = ordered[-1].nav
    total_return = float(last_nav / first_nav - Decimal("1")) if first_nav else 0.0

    peak = ordered[0].nav
    maximum_drawdown = Decimal("0")
    for snapshot in ordered:
        peak = max(peak, snapshot.nav)
        drawdown = Decimal("0") if peak == 0 else (peak - snapshot.nav) / peak
        maximum_drawdown = max(maximum_drawdown, drawdown)

    total_notional = sum((fill.notional for fill in fills), Decimal("0"))
    total_cost = sum((fill.cost for fill in fills), Decimal("0"))
    turnover = float(total_notional / first_nav) if first_nav else 0.0
    return [
        BacktestMetric(backtest_run_id, "total_return", total_return),
        BacktestMetric(backtest_run_id, "maximum_drawdown", float(maximum_drawdown)),
        BacktestMetric(backtest_run_id, "turnover", turnover),
        BacktestMetric(backtest_run_id, "total_cost", float(total_cost)),
        BacktestMetric(backtest_run_id, "trade_count", float(len(fills))),
    ]
