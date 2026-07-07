import pytest

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.registry import DuplicateFactorError, FactorRegistry


def test_registry_accepts_operator_graph_factor():
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="ret_1",
        version="1.0.0",
        namespace="price",
        description="One-bar close-to-close return.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1, Frequency.M1),
        lookback_bars=2,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )

    registry.register(spec, op.pct_change(field("close"), periods=1).alias("ret_1"))

    registered = registry.get("ret_1")

    assert registered.spec == spec
    assert registered.spec.compute_mode == ComputeMode.OPERATOR_GRAPH


def test_registry_rejects_duplicate_factor_version():
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="ma_3",
        version="1.0.0",
        namespace="price",
        description="Three-bar moving average.",
        input_fields=("close",),
        output_fields=("ma_3",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=3,
        warmup_bars=2,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )

    registry.register(spec, op.rolling_mean(field("close"), window=3).alias("ma_3"))

    with pytest.raises(DuplicateFactorError):
        registry.register(spec, op.rolling_mean(field("close"), window=3).alias("ma_3"))


def test_factor_spec_requires_output_fields():
    with pytest.raises(ValueError, match="output_fields"):
        FactorSpec(
            factor_id="bad_factor",
            version="1.0.0",
            namespace="price",
            description="Invalid factor.",
            input_fields=("close",),
            output_fields=(),
            supported_freqs=(Frequency.D1,),
            lookback_bars=1,
            warmup_bars=0,
            compute_mode=ComputeMode.OPERATOR_GRAPH,
        )
