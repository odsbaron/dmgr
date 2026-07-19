from datetime import UTC, datetime, timedelta

import pytest

from quant_research.signals import (
    AlphaScore,
    EqualWeightPortfolioBuilder,
    PortfolioConstructionConfig,
    PortfolioSelectionMode,
    SignalContractError,
)


AS_OF = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)


def score(symbol: str, value: float) -> AlphaScore:
    return AlphaScore(
        score_run_id="scores-v1",
        dataset_id="daily",
        symbol=symbol,
        freq="1d",
        as_of=AS_OF,
        available_at=AS_OF + timedelta(minutes=1),
        score=value,
        source_ref="duckdb://prediction_table?model_run_id=model-v1",
    )


def test_alpha_score_rejects_availability_before_factor_time():
    with pytest.raises(SignalContractError) as exc_info:
        AlphaScore(
            score_run_id="scores-v1",
            dataset_id="daily",
            symbol="000001.SZ",
            freq="1d",
            as_of=AS_OF,
            available_at=AS_OF - timedelta(seconds=1),
            score=1.0,
            source_ref="duckdb://feature_snapshot?factor_run_id=factor-v1",
        )

    assert exc_info.value.code == "INVALID_AVAILABILITY"


def test_top_k_is_equal_weighted_and_ties_break_by_symbol():
    targets = EqualWeightPortfolioBuilder().build(
        [score("000003.SZ", 1.0), score("000001.SZ", 1.0), score("000002.SZ", 0.5)],
        PortfolioConstructionConfig(portfolio_run_id="portfolio-v1", top_k=2),
    )

    assert [target.symbol for target in targets] == ["000001.SZ", "000003.SZ"]
    assert sum(target.target_weight for target in targets) == pytest.approx(1.0)
    assert {target.target_weight for target in targets} == {0.5}


def test_top_quantile_selects_highest_ranked_group_deterministically():
    targets = EqualWeightPortfolioBuilder().build(
        [score(f"00000{index}.SZ", float(index)) for index in range(1, 6)],
        PortfolioConstructionConfig(
            portfolio_run_id="portfolio-v1",
            selection_mode=PortfolioSelectionMode.TOP_QUANTILE,
            quantile_count=5,
            target_quantile=5,
        ),
    )

    assert [target.symbol for target in targets] == ["000005.SZ"]
