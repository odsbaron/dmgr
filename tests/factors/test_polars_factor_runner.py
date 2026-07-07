from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.polars import FactorOutputError, InputFieldError, PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry


def price_frame() -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index, close in enumerate([10.0, 11.0, 12.0, 13.0]):
        rows.append(
            {
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": start + timedelta(days=index),
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + index,
            }
        )
    return pl.DataFrame(rows).lazy()


def run_config(*factor_ids: str) -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=factor_ids,
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def register_operator_factor(
    registry: FactorRegistry,
    factor_id: str,
    expression,
    *,
    lookback_bars: int,
    warmup_bars: int,
):
    spec = FactorSpec(
        factor_id=factor_id,
        version="1.0.0",
        namespace="price",
        description=f"{factor_id} test factor.",
        input_fields=("close",),
        output_fields=(factor_id,),
        supported_freqs=(Frequency.D1, Frequency.M1),
        lookback_bars=lookback_bars,
        warmup_bars=warmup_bars,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )
    registry.register(spec, expression.alias(factor_id))


def test_operator_graph_runner_computes_return_and_rolling_mean():
    registry = FactorRegistry()
    register_operator_factor(
        registry,
        "ret_1",
        op.pct_change(field("close"), periods=1),
        lookback_bars=2,
        warmup_bars=1,
    )
    register_operator_factor(
        registry,
        "ma_3",
        op.rolling_mean(field("close"), window=3),
        lookback_bars=3,
        warmup_bars=2,
    )

    result = (
        PolarsFactorRunner(registry)
        .run(price_frame(), run_config("ret_1", "ma_3"))
        .select("symbol", "as_of", "ret_1", "ma_3")
        .collect()
    )

    assert result["ret_1"].to_list() == pytest.approx(
        [None, 0.1, 0.09090909090909091, 0.08333333333333333]
    )
    assert result["ma_3"].to_list() == [None, None, 11.0, 12.0]


def test_native_polars_expr_factor_is_marked_and_computed():
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="intrabar_ret",
        version="1.0.0",
        namespace="research",
        description="Native Polars expression factor for exploration.",
        input_fields=("open", "close"),
        output_fields=("intrabar_ret",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.POLARS_EXPR,
    )
    registry.register(
        spec,
        lambda _spec, _config: [(pl.col("close") / pl.col("open") - 1.0).alias("intrabar_ret")],
    )

    result = PolarsFactorRunner(registry).run(price_frame(), run_config("intrabar_ret")).collect()

    assert registry.get("intrabar_ret").spec.compute_mode == ComputeMode.POLARS_EXPR
    assert result["intrabar_ret"].round(6).to_list() == [0.052632, 0.047619, 0.043478, 0.04]


def test_native_polars_frame_transform_can_add_factor_column():
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="close_minus_open",
        version="1.0.0",
        namespace="research",
        description="Native Polars frame transform factor for exploration.",
        input_fields=("open", "close"),
        output_fields=("close_minus_open",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.FRAME_TRANSFORM,
    )
    registry.register(
        spec,
        lambda frame, _spec, _config: frame.with_columns(
            (pl.col("close") - pl.col("open")).alias("close_minus_open")
        ),
    )

    result = PolarsFactorRunner(registry).run(price_frame(), run_config("close_minus_open")).collect()

    assert registry.get("close_minus_open").spec.compute_mode == ComputeMode.FRAME_TRANSFORM
    assert result["close_minus_open"].to_list() == [0.5, 0.5, 0.5, 0.5]


def test_runner_rejects_missing_factor_input_field_before_compute():
    registry = FactorRegistry()
    register_operator_factor(
        registry,
        "ret_1",
        op.pct_change(field("close"), periods=1),
        lookback_bars=2,
        warmup_bars=1,
    )
    frame = pl.DataFrame({"symbol": ["000001.SZ"], "as_of": ["2026-07-01T07:00:00+00:00"]}).lazy()

    with pytest.raises(InputFieldError, match="close"):
        PolarsFactorRunner(registry).run(frame, run_config("ret_1"))


def test_runner_rejects_native_polars_factor_missing_declared_output():
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="bad_native",
        version="1.0.0",
        namespace="research",
        description="Native factor that writes the wrong output column.",
        input_fields=("close",),
        output_fields=("expected_output",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.POLARS_EXPR,
    )
    registry.register(spec, lambda _spec, _config: [pl.col("close").alias("wrong_output")])

    with pytest.raises(FactorOutputError, match="expected_output"):
        PolarsFactorRunner(registry).run(price_frame(), run_config("bad_native"))
