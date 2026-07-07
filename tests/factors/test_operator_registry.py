import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.dsl import call_op, field
from quant_research.factors.operators import OperatorRegistry, OperatorSpec, default_operator_registry
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry


def test_default_operator_registry_exposes_optimized_polars_operators():
    registry = default_operator_registry()

    pct_change = registry.get("pct_change")
    rolling_mean = registry.get("rolling_mean")

    assert pct_change.supports_streaming is False
    assert rolling_mean.requires_sorted


def test_custom_registered_operator_can_back_a_production_factor():
    operator_registry = OperatorRegistry()
    operator_registry.register(
        OperatorSpec(
            op_id="double",
            description="Example optimized operator.",
            requires_sorted=False,
            supports_streaming=True,
            compile_polars=lambda args, _params: args[0] * 2.0,
        )
    )
    factor_registry = FactorRegistry()
    factor_registry.register(
        FactorSpec(
            factor_id="double_close",
            version="1.0.0",
            namespace="price",
            description="Double the close with a registered operator.",
            input_fields=("close",),
            output_fields=("double_close",),
            supported_freqs=(Frequency.D1,),
            lookback_bars=1,
            warmup_bars=0,
            compute_mode=ComputeMode.OPERATOR_GRAPH,
        ),
        call_op("double", field("close")).alias("double_close"),
    )

    frame = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "000001.SZ"],
            "as_of": ["2026-07-01T07:00:00+00:00", "2026-07-02T07:00:00+00:00"],
            "close": [10.0, 11.0],
        }
    ).lazy()
    config = FactorRunConfig(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=("double_close",),
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )

    result = (
        PolarsFactorRunner(factor_registry, operator_registry=operator_registry)
        .run(frame, config)
        .collect()
    )

    assert result["double_close"].to_list() == [20.0, 22.0]
