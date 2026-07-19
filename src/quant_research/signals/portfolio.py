from __future__ import annotations

from collections import defaultdict

from quant_research.signals.contracts import (
    AlphaScore,
    PortfolioConstructionConfig,
    PortfolioSelectionMode,
    SignalContractError,
    TargetWeight,
)


class EqualWeightPortfolioBuilder:
    def build(
        self,
        scores: tuple[AlphaScore, ...] | list[AlphaScore],
        config: PortfolioConstructionConfig,
    ) -> tuple[TargetWeight, ...]:
        if not scores:
            raise SignalContractError("EMPTY_SCORE_SET", "scores must not be empty")

        grouped: dict[tuple[str, str, object], list[AlphaScore]] = defaultdict(list)
        seen: set[tuple[str, str, object, str]] = set()
        for score in scores:
            key = (score.dataset_id, score.freq, score.as_of, score.symbol)
            if key in seen:
                raise SignalContractError(
                    "DUPLICATE_SCORE_KEY",
                    "score set contains duplicate dataset/freq/as_of/symbol keys",
                )
            seen.add(key)
            grouped[(score.dataset_id, score.freq, score.as_of)].append(score)

        targets: list[TargetWeight] = []
        for group_key in sorted(grouped, key=lambda value: (value[0], value[1], value[2])):
            cross_section = grouped[group_key]
            selected = self._select(cross_section, config)
            if not selected:
                continue
            weight = config.gross_exposure / len(selected)
            group_available_at = max(score.available_at for score in selected)
            for score in selected:
                targets.append(
                    TargetWeight(
                        portfolio_run_id=config.portfolio_run_id,
                        dataset_id=score.dataset_id,
                        symbol=score.symbol,
                        freq=score.freq,
                        as_of=score.as_of,
                        available_at=group_available_at,
                        target_weight=weight,
                        source_score_ref=score.source_ref,
                    )
                )
        if not targets:
            raise SignalContractError("EMPTY_TARGET_SET", "portfolio produced no target weights")
        return tuple(targets)

    def _select(
        self,
        scores: list[AlphaScore],
        config: PortfolioConstructionConfig,
    ) -> list[AlphaScore]:
        ranked = sorted(scores, key=lambda score: (-score.score, score.symbol))
        if config.selection_mode == PortfolioSelectionMode.TOP_K:
            return ranked[: config.top_k]

        count = len(ranked)
        return [
            score
            for position, score in enumerate(ranked)
            if config.quantile_count - (position * config.quantile_count // count)
            == config.target_quantile
        ]
