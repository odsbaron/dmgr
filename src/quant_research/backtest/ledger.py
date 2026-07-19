from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_FLOOR

from quant_research.backtest.contracts import (
    BacktestError,
    Fill,
    NavSnapshot,
    PositionSnapshot,
    ProportionalCostConfig,
    Side,
)


class PortfolioLedger:
    def __init__(self, backtest_run_id: str, initial_cash: Decimal):
        self.backtest_run_id = backtest_run_id
        self.cash = initial_cash
        self.quantities: dict[str, int] = {}
        self.last_prices: dict[str, Decimal] = {}
        self.fills: list[Fill] = []
        self._fill_sequence = 0

    def total_value(self, prices: dict[str, Decimal] | None = None) -> Decimal:
        prices = prices or {}
        market_value = Decimal("0")
        for symbol, quantity in self.quantities.items():
            price = prices.get(symbol, self.last_prices.get(symbol))
            if price is None:
                raise BacktestError(
                    "MISSING_MARK_PRICE", f"missing mark price for held symbol {symbol}"
                )
            market_value += price * quantity
        return self.cash + market_value

    def desired_quantity(
        self,
        nav: Decimal,
        target_weight: float,
        price: Decimal,
        lot_size: int,
    ) -> int:
        if price <= 0:
            raise BacktestError("INVALID_EXECUTION_PRICE", "execution price must be positive")
        raw = (nav * Decimal(str(target_weight)) / price / lot_size).to_integral_value(
            rounding=ROUND_FLOOR
        )
        return int(raw) * lot_size

    def sell(
        self,
        *,
        symbol: str,
        quantity: int,
        price: Decimal,
        rebalance_as_of: datetime,
        execution_time: datetime,
        trading_date: date,
        costs: ProportionalCostConfig,
    ) -> Fill | None:
        held = self.quantities.get(symbol, 0)
        quantity = min(quantity, held)
        if quantity <= 0:
            return None
        notional = price * quantity
        cost = costs.calculate(Side.SELL, notional)
        self.cash += notional - cost
        remaining = held - quantity
        if remaining:
            self.quantities[symbol] = remaining
        else:
            self.quantities.pop(symbol, None)
        return self._record_fill(
            symbol=symbol,
            side=Side.SELL,
            quantity=quantity,
            price=price,
            notional=notional,
            cost=cost,
            rebalance_as_of=rebalance_as_of,
            execution_time=execution_time,
            trading_date=trading_date,
        )

    def buy(
        self,
        *,
        symbol: str,
        requested_quantity: int,
        price: Decimal,
        lot_size: int,
        rebalance_as_of: datetime,
        execution_time: datetime,
        trading_date: date,
        costs: ProportionalCostConfig,
    ) -> Fill | None:
        if requested_quantity <= 0:
            return None
        unit_cost = price * (Decimal("1") + costs.buy_rate)
        affordable_lots = int(
            (self.cash / unit_cost / lot_size).to_integral_value(rounding=ROUND_FLOOR)
        )
        quantity = min(requested_quantity, affordable_lots * lot_size)
        if quantity <= 0:
            return None
        notional = price * quantity
        cost = costs.calculate(Side.BUY, notional)
        self.cash -= notional + cost
        self.quantities[symbol] = self.quantities.get(symbol, 0) + quantity
        return self._record_fill(
            symbol=symbol,
            side=Side.BUY,
            quantity=quantity,
            price=price,
            notional=notional,
            cost=cost,
            rebalance_as_of=rebalance_as_of,
            execution_time=execution_time,
            trading_date=trading_date,
        )

    def mark_to_market(
        self,
        *,
        trading_date: date,
        as_of: datetime,
        close_prices: dict[str, Decimal],
    ) -> tuple[list[PositionSnapshot], NavSnapshot]:
        self.last_prices.update(close_prices)
        position_values: list[tuple[str, int, Decimal, Decimal]] = []
        market_value = Decimal("0")
        for symbol, quantity in sorted(self.quantities.items()):
            price = self.last_prices.get(symbol)
            if price is None:
                raise BacktestError(
                    "MISSING_MARK_PRICE", f"missing close price for held symbol {symbol}"
                )
            value = price * quantity
            market_value += value
            position_values.append((symbol, quantity, price, value))
        nav = self.cash + market_value
        positions = [
            PositionSnapshot(
                backtest_run_id=self.backtest_run_id,
                trading_date=trading_date,
                as_of=as_of,
                symbol=symbol,
                quantity=quantity,
                close_price=price,
                market_value=value,
                portfolio_weight=float(value / nav) if nav else 0.0,
            )
            for symbol, quantity, price, value in position_values
        ]
        return positions, NavSnapshot(
            backtest_run_id=self.backtest_run_id,
            trading_date=trading_date,
            as_of=as_of,
            cash=self.cash,
            market_value=market_value,
            nav=nav,
        )

    def _record_fill(
        self,
        *,
        symbol: str,
        side: Side,
        quantity: int,
        price: Decimal,
        notional: Decimal,
        cost: Decimal,
        rebalance_as_of: datetime,
        execution_time: datetime,
        trading_date: date,
    ) -> Fill:
        self._fill_sequence += 1
        fill = Fill(
            fill_id=(
                f"{self.backtest_run_id}:{execution_time.isoformat()}:"
                f"{symbol}:{side.value}:{self._fill_sequence}"
            ),
            backtest_run_id=self.backtest_run_id,
            rebalance_as_of=rebalance_as_of,
            execution_time=execution_time,
            trading_date=trading_date,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            notional=notional,
            cost=cost,
        )
        self.fills.append(fill)
        return fill
