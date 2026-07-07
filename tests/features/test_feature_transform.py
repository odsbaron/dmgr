from datetime import UTC, datetime, timedelta

import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest
from quant_research.features.transform import build_feature_snapshots, wide_to_feature_values


def factor_spec(
    factor_id: str,
    *,
    output_field: str | None = None,
    warmup_bars: int,
) -> FactorSpec:
    return FactorSpec(
        factor_id=factor_id,
        version="1.0.0",
        namespace="price",
        description=f"{factor_id} test factor.",
        input_fields=("close",),
        output_fields=(output_field or factor_id,),
        supported_freqs=(Frequency.D1,),
        lookback_bars=max(1, warmup_bars + 1),
        warmup_bars=warmup_bars,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )


def factor_frame() -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index in range(3):
        rows.append(
            {
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": start + timedelta(days=index),
                "ret_1": None if index == 0 else 0.01 * index,
                "ma_3": None if index < 2 else 10.5,
            }
        )
    return pl.DataFrame(rows).lazy()


def request() -> FeatureCommitRequest:
    return FeatureCommitRequest(
        config=FactorRunConfig(
            factor_run_id="factor-run-1",
            feature_set_id="basic_price_v1",
            input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
            factor_ids=("ret_1", "ma_3"),
            freq=Frequency.D1,
            dataset_id="fixture-daily",
        ),
        factor_frame=factor_frame(),
        resolved_factors=(
            RegisteredFactor(factor_spec("ret_1", warmup_bars=1), compute=None),
            RegisteredFactor(factor_spec("ma_3", warmup_bars=2), compute=None),
        ),
        input_row_count=3,
    )


def test_wide_to_feature_values_uses_declared_outputs_only():
    values = wide_to_feature_values(request())

    assert len(values) == 6
    assert [(value.factor_id, value.output_field) for value in values[:2]] == [
        ("ret_1", "ret_1"),
        ("ma_3", "ma_3"),
    ]
    assert values[0].value_kind == "null"
    assert values[0].warmup_complete is False
    assert values[2].factor_id == "ret_1"
    assert values[2].warmup_complete is True
    assert values[5].factor_id == "ma_3"
    assert values[5].value_float == 10.5
    assert values[5].warmup_complete is True


def test_feature_snapshots_group_features_by_symbol_and_as_of():
    values = wide_to_feature_values(request())
    snapshots = build_feature_snapshots(request().config, values)

    assert len(snapshots) == 3
    assert snapshots[0].features == {"ret_1": None, "ma_3": None}
    assert snapshots[0].warmup_complete is False
    assert snapshots[2].features == {"ret_1": 0.02, "ma_3": 10.5}
    assert snapshots[2].warmup_complete is True
    assert snapshots[2].feature_ref.startswith("duckdb://feature_snapshot?")
