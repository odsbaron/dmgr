from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from quant_research.contracts.bar import BarRecord
from quant_research.features.contracts import FeatureValue
from quant_research.labels.contracts import LabelCommitRequest, LabelSourceKind, LabelValue


@dataclass(frozen=True)
class ForwardReturnLabelConfig:
    label_run_id: str
    label_set_id: str
    label_id: str
    label_version: str
    forward_bars: int
    source_id: str
    source_kind: LabelSourceKind = LabelSourceKind.MARKET_DATA
    source_ref: str | None = None
    market_data_ref: str | None = None
    market_dataset_version: str | None = None
    market_data_definition_hash: str | None = None
    market_data_snapshot_set_hash: str | None = None

    def __post_init__(self) -> None:
        if self.forward_bars < 1:
            raise ValueError("forward_bars must be >= 1")


def forward_return_labels_from_bars(
    bars: list[BarRecord],
    config: ForwardReturnLabelConfig,
) -> LabelCommitRequest:
    created_at = datetime.now(UTC).isoformat()
    by_symbol: dict[str, list[BarRecord]] = defaultdict(list)
    for bar in bars:
        by_symbol[bar.symbol].append(bar)

    labels: list[LabelValue] = []
    for symbol in sorted(by_symbol):
        symbol_bars = sorted(by_symbol[symbol], key=lambda bar: bar.bar_end_time)
        for index, bar in enumerate(symbol_bars):
            target_index = index + config.forward_bars
            target = symbol_bars[target_index] if target_index < len(symbol_bars) else None
            value_float = (
                float(Decimal(target.close) / Decimal(bar.close) - Decimal("1"))
                if target is not None
                else None
            )
            labels.append(
                LabelValue(
                    label_run_id=config.label_run_id,
                    label_set_id=config.label_set_id,
                    dataset_id=bar.dataset_id,
                    symbol=bar.symbol,
                    freq=bar.freq.value,
                    as_of=bar.bar_end_time.isoformat(),
                    label_id=config.label_id,
                    label_version=config.label_version,
                    value_float=value_float,
                    value_string=None,
                    value_kind="null" if value_float is None else "float",
                    forward_bars=config.forward_bars,
                    source_factor_run_id=config.source_id,
                    created_at=created_at,
                    source_kind=config.source_kind,
                    source_ref=config.source_ref or config.source_id,
                )
            )

    dataset_ids = {bar.dataset_id for bar in bars}
    freqs = {bar.freq.value for bar in bars}
    as_of_values = sorted(bar.bar_end_time.isoformat() for bar in bars)
    return LabelCommitRequest(
        label_run_id=config.label_run_id,
        label_set_id=config.label_set_id,
        source_factor_run_id=config.source_id,
        labels=tuple(labels),
        source_kind=config.source_kind,
        source_ref=config.source_ref or config.source_id,
        dataset_id=next(iter(dataset_ids)) if len(dataset_ids) == 1 else None,
        freq=next(iter(freqs)) if len(freqs) == 1 else None,
        forward_bars=config.forward_bars,
        source_as_of_start=as_of_values[0] if as_of_values else None,
        source_as_of_end=as_of_values[-1] if as_of_values else None,
        market_data_ref=config.market_data_ref,
        market_dataset_version=config.market_dataset_version,
        market_data_definition_hash=config.market_data_definition_hash,
        market_data_snapshot_set_hash=config.market_data_snapshot_set_hash,
    )


def feature_values_to_label_request(
    values: tuple[FeatureValue, ...],
    *,
    label_run_id: str,
    label_set_id: str,
    forward_bars: int,
) -> LabelCommitRequest:
    if not values:
        raise ValueError("feature values must not be empty")
    source_run_ids = {value.factor_run_id for value in values}
    if len(source_run_ids) != 1:
        raise ValueError("feature values must come from exactly one factor run")
    source_factor_run_id = next(iter(source_run_ids))

    labels = tuple(
        LabelValue(
            label_run_id=label_run_id,
            label_set_id=label_set_id,
            dataset_id=value.dataset_id,
            symbol=value.symbol,
            freq=value.freq,
            as_of=value.as_of,
            label_id=value.output_field,
            label_version=value.factor_version,
            value_float=value.value_float,
            value_string=value.value_string,
            value_kind=value.value_kind,
            forward_bars=forward_bars,
            source_factor_run_id=source_factor_run_id,
            created_at=value.created_at,
            source_kind=LabelSourceKind.FACTOR_RUN,
            source_ref=f"duckdb://factor_run_manifest?factor_run_id={source_factor_run_id}",
        )
        for value in values
    )
    return LabelCommitRequest(
        label_run_id=label_run_id,
        label_set_id=label_set_id,
        source_factor_run_id=source_factor_run_id,
        labels=labels,
        source_kind=LabelSourceKind.FACTOR_RUN,
        source_ref=f"duckdb://factor_run_manifest?factor_run_id={source_factor_run_id}",
        dataset_id=values[0].dataset_id,
        freq=values[0].freq,
        forward_bars=forward_bars,
        source_as_of_start=min(value.as_of for value in values),
        source_as_of_end=max(value.as_of for value in values),
    )
