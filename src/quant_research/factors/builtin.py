from __future__ import annotations

import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.registry import FactorRegistry


ALL_FREQUENCIES = tuple(Frequency)


def default_factor_registry() -> FactorRegistry:
    registry = FactorRegistry()
    register_builtin_factors(registry)
    return registry


def register_builtin_factors(registry: FactorRegistry) -> FactorRegistry:
    _register_expr(
        registry,
        _spec("ret_1", ("close",), lookback=2, warmup=1, max_abs_value=100.0),
        pl.col("close").pct_change(n=1).over("symbol"),
    )
    _register_expr(
        registry,
        _spec("ret_5", ("close",), lookback=6, warmup=5, max_abs_value=100.0),
        pl.col("close").pct_change(n=5).over("symbol"),
    )
    _register_expr(
        registry,
        _spec("log_ret_1", ("close",), lookback=2, warmup=1, max_abs_value=100.0),
        pl.col("close").log().diff(n=1).over("symbol"),
    )
    _register_expr(
        registry,
        _spec("ma_5", ("close",), lookback=5, warmup=4),
        pl.col("close").rolling_mean(window_size=5).over("symbol"),
    )
    _register_expr(
        registry,
        _spec("ma_20", ("close",), lookback=20, warmup=19),
        pl.col("close").rolling_mean(window_size=20).over("symbol"),
    )
    _register_expr(
        registry,
        _spec(
            "close_over_ma20",
            ("close",),
            lookback=20,
            warmup=19,
            max_abs_value=100.0,
        ),
        pl.col("close") / pl.col("close").rolling_mean(window_size=20).over("symbol"),
    )
    previous_close = pl.col("close").shift(1).over("symbol")
    _register_expr(
        registry,
        _spec("true_range", ("high", "low", "close"), lookback=2, warmup=1),
        pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - previous_close).abs(),
            (pl.col("low") - previous_close).abs(),
        ),
    )
    volatility_spec = _spec(
        "volatility_20",
        ("close",),
        lookback=21,
        warmup=20,
        compute_mode=ComputeMode.FRAME_TRANSFORM,
        max_abs_value=100.0,
    )
    registry.register(volatility_spec, _volatility_20)
    return registry


def _register_expr(registry: FactorRegistry, spec: FactorSpec, expression: pl.Expr) -> None:
    registry.register(
        spec,
        lambda _spec, _config, value=expression, output=spec.factor_id: [value.alias(output)],
    )


def _volatility_20(frame: pl.LazyFrame, _spec: FactorSpec, _config) -> pl.LazyFrame:
    temporary = "__quant_research_ret_1_for_volatility_20"
    return (
        frame.with_columns(pl.col("close").pct_change(n=1).over("symbol").alias(temporary))
        .with_columns(
            pl.col(temporary).rolling_std(window_size=20).over("symbol").alias("volatility_20")
        )
        .drop(temporary)
    )


def _spec(
    factor_id: str,
    input_fields: tuple[str, ...],
    *,
    lookback: int,
    warmup: int,
    compute_mode: ComputeMode = ComputeMode.POLARS_EXPR,
    max_abs_value: float | None = None,
) -> FactorSpec:
    quality_rules: dict[str, object] = {"max_null_ratio": 1.0, "causal": True}
    if max_abs_value is not None:
        quality_rules["max_abs_value"] = max_abs_value
    return FactorSpec(
        factor_id=factor_id,
        version="1.0.0",
        namespace="builtin",
        description=f"Built-in {factor_id} factor.",
        input_fields=input_fields,
        output_fields=(factor_id,),
        supported_freqs=ALL_FREQUENCIES,
        lookback_bars=lookback,
        warmup_bars=warmup,
        compute_mode=compute_mode,
        output_dtype={factor_id: "float64"},
        quality_rules=quality_rules,
        tags=("builtin",),
    )
