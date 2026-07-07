from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl


PolarsOperatorCompiler = Callable[[list[pl.Expr], dict[str, Any]], pl.Expr]


class DuplicateOperatorError(ValueError):
    pass


class UnknownOperatorError(KeyError):
    pass


@dataclass(frozen=True)
class OperatorSpec:
    op_id: str
    description: str
    requires_sorted: bool
    supports_streaming: bool
    compile_polars: PolarsOperatorCompiler

    def __post_init__(self) -> None:
        if not self.op_id:
            raise ValueError("op_id is required")


class OperatorRegistry:
    def __init__(self):
        self._operators: dict[str, OperatorSpec] = {}

    def register(self, spec: OperatorSpec) -> None:
        if spec.op_id in self._operators:
            raise DuplicateOperatorError(f"operator already registered: {spec.op_id}")
        self._operators[spec.op_id] = spec

    def get(self, op_id: str) -> OperatorSpec:
        try:
            return self._operators[op_id]
        except KeyError as exc:
            raise UnknownOperatorError(f"unknown operator: {op_id}") from exc

    def list(self) -> list[OperatorSpec]:
        return [self._operators[op_id] for op_id in sorted(self._operators)]


def default_operator_registry() -> OperatorRegistry:
    registry = OperatorRegistry()
    registry.register(
        OperatorSpec(
            op_id="pct_change",
            description="Percent change over a symbol-local time series.",
            requires_sorted=True,
            supports_streaming=False,
            compile_polars=lambda args, params: args[0].pct_change(
                n=params.get("periods", 1)
            ).over("symbol"),
        )
    )
    registry.register(
        OperatorSpec(
            op_id="rolling_mean",
            description="Rolling mean over a symbol-local time series.",
            requires_sorted=True,
            supports_streaming=False,
            compile_polars=lambda args, params: args[0].rolling_mean(
                window_size=params["window"]
            ).over("symbol"),
        )
    )
    return registry
