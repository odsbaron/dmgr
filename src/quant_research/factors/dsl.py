from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class FactorExpression:
    kind: str
    value: Any = None
    args: tuple["FactorExpression", ...] = ()
    params: dict[str, Any] | None = None
    output_name: str | None = None

    def alias(self, name: str) -> "FactorExpression":
        return replace(self, output_name=name)

    def __add__(self, other: object) -> "FactorExpression":
        return binary("add", self, ensure_expr(other))

    def __sub__(self, other: object) -> "FactorExpression":
        return binary("sub", self, ensure_expr(other))

    def __mul__(self, other: object) -> "FactorExpression":
        return binary("mul", self, ensure_expr(other))

    def __truediv__(self, other: object) -> "FactorExpression":
        return binary("truediv", self, ensure_expr(other))

    def __radd__(self, other: object) -> "FactorExpression":
        return binary("add", ensure_expr(other), self)

    def __rsub__(self, other: object) -> "FactorExpression":
        return binary("sub", ensure_expr(other), self)

    def __rmul__(self, other: object) -> "FactorExpression":
        return binary("mul", ensure_expr(other), self)

    def __rtruediv__(self, other: object) -> "FactorExpression":
        return binary("truediv", ensure_expr(other), self)


def field(name: str) -> FactorExpression:
    return FactorExpression(kind="field", value=name)


def literal(value: object) -> FactorExpression:
    return FactorExpression(kind="literal", value=value)


def binary(op_name: str, left: FactorExpression, right: FactorExpression) -> FactorExpression:
    return FactorExpression(kind="binary", value=op_name, args=(left, right))


def call_op(op_id: str, *args: FactorExpression, **params: object) -> FactorExpression:
    return FactorExpression(kind="operator", value=op_id, args=args, params=params)


def ensure_expr(value: object) -> FactorExpression:
    if isinstance(value, FactorExpression):
        return value
    return literal(value)


class OperatorNamespace:
    def pct_change(self, expr: FactorExpression, *, periods: int = 1) -> FactorExpression:
        return call_op("pct_change", expr, periods=periods)

    def rolling_mean(self, expr: FactorExpression, *, window: int) -> FactorExpression:
        return call_op("rolling_mean", expr, window=window)


op = OperatorNamespace()
