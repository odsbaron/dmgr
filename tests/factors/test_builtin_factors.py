from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from quant_research.contracts.bar import Frequency
from quant_research.factors.builtin import default_factor_registry
from quant_research.factors.contracts import FactorContext, FactorRunConfig
from quant_research.factors.polars import PolarsFactorRunner


BUILTIN_IDS = (
    "ret_1",
    "ret_5",
    "log_ret_1",
    "ma_5",
    "ma_20",
    "close_over_ma20",
    "true_range",
    "volatility_20",
)


def frame(freq: Frequency) -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 1, 30, tzinfo=UTC)
    step = timedelta(minutes=1) if freq == Frequency.M1 else timedelta(days=1)
    rows = []
    for index in range(25):
        close = 10.0 + index
        rows.append(
            {
                "dataset_id": "fixture",
                "symbol": "000001.SZ",
                "freq": freq.value,
                "as_of": start + step * index,
                "open": close - 0.25,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1000.0 + index,
            }
        )
    return pl.DataFrame(rows).lazy()


def config(freq: Frequency, *factor_ids: str) -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id=f"builtin-{freq.value}",
        feature_set_id="builtin-v1",
        input_data_ref=f"duckdb://curated_market_bar?dataset_id=fixture&freq={freq.value}",
        factor_ids=tuple(factor_ids),
        freq=freq,
        dataset_id="fixture",
    )


def test_default_registry_contains_complete_builtin_catalog_and_factor_context():
    registry = default_factor_registry()
    run_config = config(Frequency.D1, "ret_1")

    assert tuple(spec.factor_id for spec in registry.list()) == tuple(sorted(BUILTIN_IDS))
    assert FactorContext.from_run_config(run_config) == FactorContext(
        input_data_ref=run_config.input_data_ref,
        dataset_id="fixture",
        freq=Frequency.D1,
    )


def test_builtin_factor_values_and_warmup_windows():
    registry = default_factor_registry()
    result = (
        PolarsFactorRunner(registry)
        .run(frame(Frequency.D1), config(Frequency.D1, *BUILTIN_IDS))
        .collect()
    )

    assert result["ret_1"][1] == pytest.approx(0.1)
    assert result["ret_5"][5] == pytest.approx(0.5)
    assert result["log_ret_1"][1] == pytest.approx(__import__("math").log(11.0 / 10.0))
    assert result["ma_5"][4] == pytest.approx(12.0)
    assert result["ma_20"][19] == pytest.approx(19.5)
    assert result["close_over_ma20"][19] == pytest.approx(29.0 / 19.5)
    assert result["true_range"][1] == pytest.approx(1.5)
    assert result["volatility_20"][20] is not None


@pytest.mark.parametrize("freq", [Frequency.D1, Frequency.M1])
def test_builtin_factor_output_preserves_frequency_specific_as_of(freq):
    registry = default_factor_registry()
    source = frame(freq).collect()
    result = PolarsFactorRunner(registry).run(frame(freq), config(freq, "ret_1")).collect()

    assert result["freq"].unique().to_list() == [freq.value]
    assert result["as_of"].to_list() == source["as_of"].to_list()
    if freq == Frequency.M1:
        assert result["as_of"][1] - result["as_of"][0] == timedelta(minutes=1)
