from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from quant_research.factors.contracts import ComputeMode, FactorRunConfig
from quant_research.factors.dsl import FactorExpression
from quant_research.factors.operators import OperatorRegistry, default_operator_registry
from quant_research.factors.registry import FactorRegistry


class UnsupportedExpressionError(ValueError):
    pass


class InputFieldError(ValueError):
    pass


class FactorOutputError(ValueError):
    pass


@dataclass(frozen=True)
class PolarsExpressionCompiler:
    group_field: str = "symbol"
    operator_registry: OperatorRegistry | None = None

    def __post_init__(self) -> None:
        if self.operator_registry is None:
            object.__setattr__(self, "operator_registry", default_operator_registry())

    def compile(self, expression: FactorExpression) -> pl.Expr:
        compiled = self._compile(expression)
        if expression.output_name:
            return compiled.alias(expression.output_name)
        return compiled

    def _compile(self, expression: FactorExpression) -> pl.Expr:
        if expression.kind == "field":
            return pl.col(expression.value)
        if expression.kind == "literal":
            return pl.lit(expression.value)
        if expression.kind == "binary":
            left = self._compile(expression.args[0])
            right = self._compile(expression.args[1])
            return self._compile_binary(expression.value, left, right)
        if expression.kind == "operator":
            return self._compile_operator(expression)
        raise UnsupportedExpressionError(f"unsupported factor expression kind: {expression.kind}")

    def _compile_binary(self, op_name: str, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        if op_name == "add":
            return left + right
        if op_name == "sub":
            return left - right
        if op_name == "mul":
            return left * right
        if op_name == "truediv":
            return left / right
        raise UnsupportedExpressionError(f"unsupported binary operator: {op_name}")

    def _compile_operator(self, expression: FactorExpression) -> pl.Expr:
        params = expression.params or {}
        args = [self._compile(arg) for arg in expression.args]
        operator_registry = self.operator_registry
        if operator_registry is None:
            raise UnsupportedExpressionError("operator registry is not configured")
        return operator_registry.get(expression.value).compile_polars(args, params)


class PolarsFactorRunner:
    def __init__(
        self,
        registry: FactorRegistry,
        *,
        operator_registry: OperatorRegistry | None = None,
    ):
        self._registry = registry
        self._compiler = PolarsExpressionCompiler(operator_registry=operator_registry)

    def run(self, frame: pl.LazyFrame, config: FactorRunConfig) -> pl.LazyFrame:
        result = frame.sort(["symbol", "as_of"])
        for registered in self._registry.resolve_many(config.factor_ids, freq=config.freq):
            spec = registered.spec
            compute = registered.compute
            self._validate_inputs(result, spec.input_fields, spec.factor_id)
            if spec.compute_mode == ComputeMode.OPERATOR_GRAPH:
                result = result.with_columns(self._compiler.compile(compute))
            elif spec.compute_mode == ComputeMode.POLARS_EXPR:
                result = result.with_columns(compute(spec, config))
            elif spec.compute_mode == ComputeMode.FRAME_TRANSFORM:
                result = compute(result, spec, config)
            else:
                raise ValueError(f"unsupported Polars factor compute mode: {spec.compute_mode}")
            self._validate_outputs(result, spec.output_fields, spec.factor_id)
        return result

    def _validate_inputs(
        self,
        frame: pl.LazyFrame,
        input_fields: tuple[str, ...],
        factor_id: str,
    ) -> None:
        columns = set(frame.collect_schema().names())
        missing = sorted(set(input_fields) - columns)
        if missing:
            raise InputFieldError(
                f"factor {factor_id} is missing input fields: {', '.join(missing)}"
            )

    def _validate_outputs(
        self,
        frame: pl.LazyFrame,
        output_fields: tuple[str, ...],
        factor_id: str,
    ) -> None:
        columns = set(frame.collect_schema().names())
        missing = sorted(set(output_fields) - columns)
        if missing:
            raise FactorOutputError(
                f"factor {factor_id} did not produce output fields: {', '.join(missing)}"
            )
