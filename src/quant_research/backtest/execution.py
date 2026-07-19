from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from quant_research.backtest.contracts import Side
from quant_research.contracts.bar import BarRecord


class TradingEligibility(Protocol):
    def is_eligible(self, bar: BarRecord, side: Side) -> bool: ...


class AllowValidPriceEligibility:
    def is_eligible(self, bar: BarRecord, side: Side) -> bool:
        del side
        return Decimal(bar.open) > 0 and Decimal(bar.volume) > 0
