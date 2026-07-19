from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal

from quant_research import __version__
from quant_research.backtest.analytics import analyze_backtest
from quant_research.backtest.contracts import (
    BacktestError,
    BacktestRunManifest,
    BacktestRunResult,
    BacktestRunStatus,
    DailyBacktestRequest,
    Side,
)
from quant_research.backtest.duckdb_store import LocalDuckDBBacktestStore
from quant_research.backtest.execution import AllowValidPriceEligibility, TradingEligibility
from quant_research.backtest.ledger import PortfolioLedger
from quant_research.contracts.bar import BarRecord
from quant_research.signals.contracts import TargetWeight


class DailyBacktestPipeline:
    def __init__(
        self,
        store: LocalDuckDBBacktestStore,
        *,
        eligibility: TradingEligibility | None = None,
    ):
        self.store = store
        self.eligibility = eligibility or AllowValidPriceEligibility()

    def run(self, request: DailyBacktestRequest) -> BacktestRunResult:
        config_hash = self._config_hash(request)
        existing = self.store.get_manifest(request.backtest_run_id)
        if existing is not None:
            if existing.config_hash != config_hash:
                from quant_research.backtest.contracts import BacktestConflictError

                raise BacktestConflictError(
                    "BACKTEST_RUN_CONFLICT",
                    "backtest_run_id already exists with a different config hash",
                )
            return self.store.commit_run(
                existing,
                fills=[],
                positions=[],
                nav_snapshots=[],
                metrics=[],
            )

        started_at = datetime.now(UTC).isoformat()
        fills, positions, nav_snapshots = self._simulate(request)
        metrics = analyze_backtest(
            request.backtest_run_id,
            nav_snapshots,
            fills,
            initial_cash=request.initial_cash,
        )
        finished_at = datetime.now(UTC).isoformat()
        manifest = BacktestRunManifest(
            backtest_run_id=request.backtest_run_id,
            target_source_ref=request.target_source_ref,
            market_data_ref=request.market_data_ref,
            initial_cash=request.initial_cash,
            execution_config={
                "price_field": request.execution.price_field,
                "lot_size": request.execution.lot_size,
                "convention": request.execution.convention,
            },
            cost_config={
                "buy_rate": str(request.costs.buy_rate),
                "sell_rate": str(request.costs.sell_rate),
            },
            status=BacktestRunStatus.COMMITTED,
            started_at=started_at,
            finished_at=finished_at,
            config_hash=config_hash,
            code_version=__version__,
            row_count_fill=len(fills),
            row_count_position=len(positions),
            row_count_nav=len(nav_snapshots),
            row_count_metric=len(metrics),
            universe_ref=request.universe_ref,
            calendar_ref=request.calendar_ref,
            daily_status_ref=request.daily_status_ref,
            coverage_report_ref=request.coverage_report_ref,
        )
        return self.store.commit_run(
            manifest,
            fills=fills,
            positions=positions,
            nav_snapshots=nav_snapshots,
            metrics=metrics,
        )

    def _simulate(self, request: DailyBacktestRequest):
        bars_by_date: dict[date, dict[str, BarRecord]] = defaultdict(dict)
        for bar in request.bars:
            if bar.dataset_id not in {target.dataset_id for target in request.target_weights}:
                continue
            if bar.symbol in bars_by_date[bar.trading_date]:
                raise BacktestError(
                    "DUPLICATE_DAILY_BAR",
                    f"duplicate daily bar for {bar.symbol} on {bar.trading_date.isoformat()}",
                )
            bars_by_date[bar.trading_date][bar.symbol] = bar
        if not bars_by_date:
            raise BacktestError("NO_MATCHING_BARS", "no bars match the target dataset")

        targets_by_as_of: dict[datetime, list[TargetWeight]] = defaultdict(list)
        for target in request.target_weights:
            targets_by_as_of[target.as_of].append(target)

        events_by_date: dict[date, list[tuple[datetime, list[TargetWeight]]]] = defaultdict(list)
        ordered_dates = sorted(bars_by_date)
        for as_of in sorted(targets_by_as_of):
            targets = targets_by_as_of[as_of]
            available_at = max(target.available_at for target in targets)
            execution_date = self._next_execution_date(
                ordered_dates,
                bars_by_date,
                as_of=as_of,
                available_at=available_at,
            )
            if execution_date is None:
                continue
            events_by_date[execution_date].append((as_of, targets))

        ledger = PortfolioLedger(request.backtest_run_id, request.initial_cash)
        all_positions = []
        nav_snapshots = []
        for trading_date in ordered_dates:
            daily_bars = bars_by_date[trading_date]
            for as_of, targets in sorted(
                events_by_date.get(trading_date, []), key=lambda item: item[0]
            ):
                self._execute_rebalance(request, ledger, as_of, targets, daily_bars)
            close_prices = {symbol: Decimal(bar.close) for symbol, bar in daily_bars.items()}
            as_of = max(bar.bar_end_time for bar in daily_bars.values())
            positions, nav = ledger.mark_to_market(
                trading_date=trading_date,
                as_of=as_of,
                close_prices=close_prices,
            )
            all_positions.extend(positions)
            nav_snapshots.append(nav)
        return ledger.fills, all_positions, nav_snapshots

    def _next_execution_date(
        self,
        ordered_dates: list[date],
        bars_by_date: dict[date, dict[str, BarRecord]],
        *,
        as_of: datetime,
        available_at: datetime,
    ) -> date | None:
        for trading_date in ordered_dates:
            bars = bars_by_date[trading_date].values()
            if any(bar.bar_end_time > as_of and bar.bar_start_time >= available_at for bar in bars):
                return trading_date
        return None

    def _execute_rebalance(
        self,
        request: DailyBacktestRequest,
        ledger: PortfolioLedger,
        rebalance_as_of: datetime,
        targets: list[TargetWeight],
        daily_bars: dict[str, BarRecord],
    ) -> None:
        target_by_symbol = {target.symbol: target for target in targets}
        open_prices = {symbol: Decimal(bar.open) for symbol, bar in daily_bars.items()}
        nav_at_open = ledger.total_value(open_prices)
        desired: dict[str, int] = {}
        for symbol in sorted(set(ledger.quantities) | set(target_by_symbol)):
            target = target_by_symbol.get(symbol)
            bar = daily_bars.get(symbol)
            if target is None:
                desired[symbol] = 0
            elif bar is not None and self._bar_is_after_target(bar, target):
                desired[symbol] = ledger.desired_quantity(
                    nav_at_open,
                    target.target_weight,
                    Decimal(bar.open),
                    request.execution.lot_size,
                )
            else:
                desired[symbol] = ledger.quantities.get(symbol, 0)

        for symbol in sorted(desired):
            current = ledger.quantities.get(symbol, 0)
            if current <= desired[symbol]:
                continue
            bar = daily_bars.get(symbol)
            if bar is None or not self._bar_is_after_group(bar, rebalance_as_of, targets):
                continue
            if not self.eligibility.is_eligible(bar, Side.SELL):
                continue
            ledger.sell(
                symbol=symbol,
                quantity=current - desired[symbol],
                price=Decimal(bar.open),
                rebalance_as_of=rebalance_as_of,
                execution_time=bar.bar_start_time,
                trading_date=bar.trading_date,
                costs=request.costs,
            )

        for symbol in sorted(desired):
            current = ledger.quantities.get(symbol, 0)
            if current >= desired[symbol]:
                continue
            bar = daily_bars.get(symbol)
            target = target_by_symbol.get(symbol)
            if bar is None or target is None or not self._bar_is_after_target(bar, target):
                continue
            if not self.eligibility.is_eligible(bar, Side.BUY):
                continue
            ledger.buy(
                symbol=symbol,
                requested_quantity=desired[symbol] - current,
                price=Decimal(bar.open),
                lot_size=request.execution.lot_size,
                rebalance_as_of=rebalance_as_of,
                execution_time=bar.bar_start_time,
                trading_date=bar.trading_date,
                costs=request.costs,
            )

    def _bar_is_after_target(self, bar: BarRecord, target: TargetWeight) -> bool:
        return bar.bar_end_time > target.as_of and bar.bar_start_time >= target.available_at

    def _bar_is_after_group(
        self,
        bar: BarRecord,
        rebalance_as_of: datetime,
        targets: list[TargetWeight],
    ) -> bool:
        available_at = max(target.available_at for target in targets)
        return bar.bar_end_time > rebalance_as_of and bar.bar_start_time >= available_at

    def _config_hash(self, request: DailyBacktestRequest) -> str:
        payload = {
            "target_source_ref": request.target_source_ref,
            "market_data_ref": request.market_data_ref,
            "initial_cash": str(request.initial_cash),
            "execution": {
                "price_field": request.execution.price_field,
                "lot_size": request.execution.lot_size,
                "convention": request.execution.convention,
            },
            "costs": {
                "buy_rate": str(request.costs.buy_rate),
                "sell_rate": str(request.costs.sell_rate),
            },
            "universe_ref": request.universe_ref,
            "calendar_ref": request.calendar_ref,
            "daily_status_ref": request.daily_status_ref,
            "coverage_report_ref": request.coverage_report_ref,
            "targets": [
                {
                    "portfolio_run_id": target.portfolio_run_id,
                    "dataset_id": target.dataset_id,
                    "symbol": target.symbol,
                    "freq": target.freq,
                    "as_of": target.as_of.isoformat(),
                    "available_at": target.available_at.isoformat(),
                    "target_weight": target.target_weight,
                    "source_score_ref": target.source_score_ref,
                }
                for target in sorted(
                    request.target_weights,
                    key=lambda item: (item.as_of, item.symbol),
                )
            ],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
