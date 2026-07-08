from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl

from quant_research.contracts.refs import DataRef
from quant_research.features.contracts import FeatureSnapshot
from quant_research.features.gates import FeatureQualityGate


_KEY_COLUMNS = [
    "feature_set_id",
    "dataset_id",
    "symbol",
    "freq",
    "as_of",
    "warmup_complete",
    "feature_ref",
]


@dataclass(frozen=True)
class TrainingFeatureMatrixBuilder:
    quality_gate: FeatureQualityGate

    def build(
        self,
        snapshot_ref: DataRef | str,
        *,
        feature_fields: tuple[str, ...] | None = None,
    ) -> pl.LazyFrame:
        snapshots = self.quality_gate.read_consumable_snapshot(snapshot_ref)
        return snapshots_to_feature_matrix(snapshots, feature_fields=feature_fields)


def snapshots_to_feature_matrix(
    snapshots: Iterable[FeatureSnapshot],
    *,
    feature_fields: tuple[str, ...] | None = None,
) -> pl.LazyFrame:
    snapshot_list = list(snapshots)
    if not snapshot_list:
        raise ValueError("snapshots must not be empty")

    fields = list(feature_fields) if feature_fields is not None else _feature_fields(snapshot_list)
    rows = [_snapshot_to_row(snapshot, fields) for snapshot in snapshot_list]
    return pl.DataFrame(rows).select([*_KEY_COLUMNS, *fields]).lazy()


def _feature_fields(snapshots: list[FeatureSnapshot]) -> list[str]:
    fields = {field for snapshot in snapshots for field in snapshot.features}
    return sorted(fields)


def _snapshot_to_row(snapshot: FeatureSnapshot, feature_fields: list[str]) -> dict[str, object]:
    row: dict[str, object] = {
        "feature_set_id": snapshot.feature_set_id,
        "dataset_id": snapshot.dataset_id,
        "symbol": snapshot.symbol,
        "freq": snapshot.freq,
        "as_of": snapshot.as_of,
        "warmup_complete": snapshot.warmup_complete,
        "feature_ref": snapshot.feature_ref,
    }
    for field in feature_fields:
        row[field] = snapshot.features.get(field)
    return row
