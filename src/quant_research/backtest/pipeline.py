from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import asdict
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
        content_hash = self._content_hash(fills, positions, nav_snapshots, metrics)
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
            content_hash=content_hash,
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
        target_dataset = request.target_weights[0].dataset_id
        for bar in request.bars:
            if bar.dataset_id != target_dataset:
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
        ordered_dates = sorted(bars_by_date)

        ledger = PortfolioLedger(request.backtest_run_id, request.initial_cash)
        all_positions = []
        nav_snapshots = []
        remaining_snapshots = dict(targets_by_as_of)
        active_snapshot: tuple[datetime, list[TargetWeight]] | None = None
        pending_symbols: set[str] = set()
        for trading_date in ordered_dates:
            daily_bars = bars_by_date[trading_date]
            ready_as_of = [
                as_of
                for as_of, targets in remaining_snapshots.items()
                if self._group_is_ready(daily_bars, as_of, targets)
            ]
            if ready_as_of:
                latest_as_of = max(ready_as_of)
                active_snapshot = (latest_as_of, remaining_snapshots[latest_as_of])
                remaining_snapshots = {
                    as_of: targets
                    for as_of, targets in remaining_snapshots.items()
                    if as_of > latest_as_of
                }
                pending_symbols = set(ledger.quantities) | {
                    target.symbol for target in active_snapshot[1]
                }

            if active_snapshot is not None and pending_symbols:
                rebalance_as_of, targets = active_snapshot
                pending_symbols = self._execute_rebalance(
                    request,
                    ledger,
                    rebalance_as_of,
                    targets,
                    daily_bars,
                    pending_symbols,
                )
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

    def _group_is_ready(
        self,
        daily_bars: dict[str, BarRecord],
        rebalance_as_of: datetime,
        targets: list[TargetWeight],
    ) -> bool:
        return any(
            self._bar_is_after_group(bar, rebalance_as_of, targets) for bar in daily_bars.values()
        )

    def _execute_rebalance(
        self,
        request: DailyBacktestRequest,
        ledger: PortfolioLedger,
        rebalance_as_of: datetime,
        targets: list[TargetWeight],
        daily_bars: dict[str, BarRecord],
        pending_symbols: set[str],
    ) -> set[str]:
        target_by_symbol = {target.symbol: target for target in targets}
        open_prices = {symbol: Decimal(bar.open) for symbol, bar in daily_bars.items()}
        nav_at_open = ledger.total_value(open_prices)
        desired: dict[str, int] = {}
        actionable: set[str] = set()
        remaining = set(pending_symbols)
        for symbol in sorted(pending_symbols):
            target = target_by_symbol.get(symbol)
            bar = daily_bars.get(symbol)
            if bar is None:
                continue
            if target is None:
                if not self._bar_is_after_group(bar, rebalance_as_of, targets):
                    continue
                desired[symbol] = 0
            else:
                if not self._bar_is_after_target(bar, target):
                    continue
                desired[symbol] = ledger.desired_quantity(
                    nav_at_open,
                    target.target_weight,
                    Decimal(bar.open),
                    request.execution.lot_size,
                )
            actionable.add(symbol)

        for symbol in sorted(actionable):
            current = ledger.quantities.get(symbol, 0)
            if current <= desired[symbol]:
                continue
            bar = daily_bars.get(symbol)
            assert bar is not None
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
            if ledger.quantities.get(symbol, 0) == desired[symbol]:
                remaining.discard(symbol)

        for symbol in sorted(actionable):
            current = ledger.quantities.get(symbol, 0)
            if current >= desired[symbol]:
                if current == desired[symbol]:
                    remaining.discard(symbol)
                continue
            bar = daily_bars.get(symbol)
            target = target_by_symbol.get(symbol)
            assert bar is not None and target is not None
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
            if ledger.quantities.get(symbol, 0) >= desired[symbol]:
                remaining.discard(symbol)
        return remaining

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
            "code_version": __version__,
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
            "eligibility": {
                "type": (
                    f"{type(self.eligibility).__module__}.{type(self.eligibility).__qualname__}"
                ),
                "state": self._canonical_value(getattr(self.eligibility, "__dict__", {})),
            },
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
            "bars": [
                bar.to_dict()
                for bar in sorted(
                    request.bars,
                    key=lambda item: (
                        item.dataset_id,
                        item.trading_date,
                        item.symbol,
                        item.bar_start_time,
                        item.source_run_id,
                        item.source_row_id or "",
                    ),
                )
            ],
        }
        return self._sha256(payload)

    def _content_hash(self, fills, positions, nav_snapshots, metrics) -> str:
        return self._sha256(
            {
                "fills": [asdict(fill) for fill in fills],
                "positions": [asdict(position) for position in positions],
                "nav": [asdict(snapshot) for snapshot in nav_snapshots],
                "metrics": [asdict(metric) for metric in metrics],
            }
        )

    def _sha256(self, payload: object) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    def _canonical_value(self, value: object) -> object:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, dict):
            return {
                str(key): self._canonical_value(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        if isinstance(value, (list, tuple)):
            return [self._canonical_value(item) for item in value]
        return repr(value)
