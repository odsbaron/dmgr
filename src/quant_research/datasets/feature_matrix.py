from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl

from quant_research.contracts.refs import DataRef
from quant_research.datasets.contracts import (
    TrainingDatasetError,
    TrainingDatasetManifest,
    TrainingDatasetStatus,
    training_dataset_hash,
)
from quant_research.datasets.duckdb_store import LocalDuckDBTrainingDatasetStore
from quant_research.features.contracts import FeatureRunManifest
from quant_research.features.contracts import FeatureSnapshot
from quant_research.features.gates import FeatureQualityGate
from quant_research.labels.contracts import LabelRunManifest, LabelValue
from quant_research.labels.gates import LabelQualityGate


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
    label_gate: LabelQualityGate | None = None
    dataset_store: LocalDuckDBTrainingDatasetStore | None = None

    def build(
        self,
        snapshot_ref: DataRef | str,
        *,
        feature_fields: tuple[str, ...] | None = None,
    ) -> pl.LazyFrame:
        snapshots = self.quality_gate.read_consumable_snapshot(snapshot_ref)
        return snapshots_to_feature_matrix(snapshots, feature_fields=feature_fields)

    def build_with_labels(
        self,
        snapshot_ref: DataRef | str,
        label_ref: DataRef | str,
        *,
        feature_fields: tuple[str, ...] | None = None,
        label_fields: tuple[str, ...] | None = None,
    ) -> pl.LazyFrame:
        if self.label_gate is None:
            raise ValueError("label_gate is required to build a labeled feature matrix")
        snapshots, labels, _, _ = self._read_compatible_inputs(snapshot_ref, label_ref)
        feature_frame = snapshots_to_feature_matrix(snapshots, feature_fields=feature_fields)
        label_frame = labels_to_label_matrix(labels, label_fields=label_fields)
        return feature_frame.join(label_frame, on=["dataset_id", "symbol", "freq", "as_of"], how="inner")

    def build_manifested(
        self,
        training_dataset_id: str,
        snapshot_ref: DataRef | str,
        label_ref: DataRef | str,
        *,
        feature_fields: tuple[str, ...] | None = None,
        label_fields: tuple[str, ...] | None = None,
    ) -> "TrainingDatasetBuildResult":
        if not training_dataset_id:
            raise ValueError("training_dataset_id is required")
        if self.dataset_store is None:
            raise ValueError("dataset_store is required for manifested assembly")

        snapshots, labels, feature_manifest, label_manifest = self._read_compatible_inputs(
            snapshot_ref,
            label_ref,
        )
        selected_features = feature_fields or tuple(_feature_fields(list(snapshots)))
        selected_labels = label_fields or tuple(_label_fields(list(labels)))
        overlap = sorted(set(selected_features) & set(selected_labels))
        if overlap:
            raise TrainingDatasetError(
                "FIELD_NAME_COLLISION",
                f"feature and label fields overlap: {', '.join(overlap)}",
            )

        feature_frame = snapshots_to_feature_matrix(
            snapshots,
            feature_fields=selected_features,
        ).collect()
        label_frame = labels_to_label_matrix(labels, label_fields=selected_labels).collect()
        keys = ["dataset_id", "symbol", "freq", "as_of"]
        joined = feature_frame.join(label_frame, on=keys, how="inner")
        feature_only = feature_frame.select(keys).join(label_frame.select(keys), on=keys, how="anti")
        label_only = label_frame.select(keys).join(feature_frame.select(keys), on=keys, how="anti")

        canonical_feature_ref = _as_ref(snapshot_ref).uri
        canonical_label_ref = _as_ref(label_ref).uri
        content_hash = training_dataset_hash(
            {
                "training_dataset_id": training_dataset_id,
                "feature_ref": canonical_feature_ref,
                "label_ref": canonical_label_ref,
                "feature_fields": list(selected_features),
                "label_fields": list(selected_labels),
                "market_data_definition_hash": feature_manifest.market_data_definition_hash,
                "universe_snapshot_set_hash": feature_manifest.universe_snapshot_set_hash,
                "rows": joined.sort(keys).to_dicts(),
            }
        )
        manifest = TrainingDatasetManifest(
            training_dataset_id=training_dataset_id,
            feature_ref=canonical_feature_ref,
            label_ref=canonical_label_ref,
            factor_run_id=feature_manifest.factor_run_id,
            label_run_id=label_manifest.label_run_id,
            feature_set_id=feature_manifest.feature_set_id,
            label_set_id=label_manifest.label_set_id,
            dataset_id=feature_manifest.dataset_id,
            freq=feature_manifest.freq,
            feature_fields=selected_features,
            label_fields=selected_labels,
            row_count_feature=feature_frame.height,
            row_count_label=label_frame.height,
            row_count_joined=joined.height,
            row_count_feature_only=feature_only.height,
            row_count_label_only=label_only.height,
            content_hash=content_hash,
            status=TrainingDatasetStatus.COMMITTED,
            created_at=datetime.now(UTC).isoformat(),
            market_data_definition_hash=feature_manifest.market_data_definition_hash,
            feature_market_data_snapshot_set_hash=(
                feature_manifest.market_data_snapshot_set_hash
            ),
            label_market_data_snapshot_set_hash=(
                label_manifest.market_data_snapshot_set_hash
            ),
            universe_ref=feature_manifest.universe_ref,
            universe_id=feature_manifest.universe_id,
            universe_version=feature_manifest.universe_version,
            universe_definition_hash=feature_manifest.universe_definition_hash,
            universe_snapshot_set_hash=feature_manifest.universe_snapshot_set_hash,
            label_source_kind=label_manifest.source_kind.value,
            label_source_ref=label_manifest.source_ref,
            label_forward_bars=label_manifest.forward_bars,
        )
        commit = self.dataset_store.commit_manifest(manifest)
        return TrainingDatasetBuildResult(
            matrix=joined.lazy(),
            manifest=commit.manifest,
            reused_existing=commit.reused_existing,
        )

    def _read_compatible_inputs(
        self,
        snapshot_ref: DataRef | str,
        label_ref: DataRef | str,
    ) -> tuple[list[FeatureSnapshot], list[LabelValue], FeatureRunManifest, LabelRunManifest]:
        if self.label_gate is None:
            raise ValueError("label_gate is required to build a labeled feature matrix")
        snapshots = self.quality_gate.read_consumable_snapshot(snapshot_ref)
        labels = self.label_gate.read_consumable_labels(label_ref)
        feature_ref = _as_ref(snapshot_ref)
        labels_ref = _as_ref(label_ref)
        factor_run_id = feature_ref.filters.get("factor_run_id")
        label_run_id = labels_ref.filters.get("label_run_id")
        feature_manifest = self.quality_gate.feature_store.get_manifest(factor_run_id or "")
        label_manifest = self.label_gate.label_store.get_manifest(label_run_id or "")
        if feature_manifest is None or label_manifest is None:
            raise TrainingDatasetError(
                "MISSING_INPUT_MANIFEST",
                "feature and label manifests are required for training assembly",
            )
        _assert_compatible(feature_manifest, label_manifest, snapshots)
        return snapshots, labels, feature_manifest, label_manifest


@dataclass(frozen=True)
class TrainingDatasetBuildResult:
    matrix: pl.LazyFrame
    manifest: TrainingDatasetManifest
    reused_existing: bool = False


def _assert_compatible(
    feature: FeatureRunManifest,
    label: LabelRunManifest,
    snapshots: list[FeatureSnapshot],
) -> None:
    if label.dataset_id is not None and feature.dataset_id != label.dataset_id:
        raise TrainingDatasetError("DATASET_ID_MISMATCH", "feature and label dataset ids differ")
    if label.freq is not None and feature.freq != label.freq:
        raise TrainingDatasetError("FREQUENCY_MISMATCH", "feature and label frequencies differ")
    if (
        feature.market_data_definition_hash is not None
        and label.market_data_definition_hash is not None
        and feature.market_data_definition_hash != label.market_data_definition_hash
    ):
        raise TrainingDatasetError(
            "MARKET_DATA_DEFINITION_MISMATCH",
            "feature and label market-data definitions differ",
        )
    if label.universe_ref is not None:
        feature_lineage = (
            feature.universe_id,
            feature.universe_version,
            feature.universe_definition_hash,
            feature.universe_snapshot_set_hash,
        )
        label_lineage = (
            label.universe_id,
            label.universe_version,
            label.universe_definition_hash,
            label.universe_snapshot_set_hash,
        )
        if feature.universe_ref is None or feature_lineage != label_lineage:
            raise TrainingDatasetError(
                "UNIVERSE_LINEAGE_MISMATCH",
                "declared feature and label Universe lineage differs",
            )
    if snapshots and label.source_as_of_start is not None and label.source_as_of_end is not None:
        feature_start = min(_parse_as_of(snapshot.as_of) for snapshot in snapshots)
        feature_end = max(_parse_as_of(snapshot.as_of) for snapshot in snapshots)
        if _parse_as_of(label.source_as_of_start) > feature_start:
            raise TrainingDatasetError(
                "LABEL_SOURCE_RANGE_MISMATCH",
                "label source begins after the first feature observation",
            )
        if _parse_as_of(label.source_as_of_end) < feature_end:
            raise TrainingDatasetError(
                "LABEL_SOURCE_RANGE_MISMATCH",
                "label source ends before the last feature observation",
            )
        if (
            label.market_data_ref is not None
            and label.forward_bars is not None
            and label.forward_bars > 0
            and _parse_as_of(label.source_as_of_end) <= feature_end
        ):
            raise TrainingDatasetError(
                "LABEL_FORWARD_HORIZON_NOT_COVERED",
                "exact label source must extend beyond the final feature observation",
            )


def _parse_as_of(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _as_ref(value: DataRef | str) -> DataRef:
    return DataRef.parse(value) if isinstance(value, str) else value


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


def labels_to_label_matrix(
    labels: Iterable[LabelValue],
    *,
    label_fields: tuple[str, ...] | None = None,
) -> pl.LazyFrame:
    label_list = list(labels)
    if not label_list:
        raise ValueError("labels must not be empty")

    fields = list(label_fields) if label_fields is not None else _label_fields(label_list)
    rows = _label_rows(label_list, fields)
    return pl.DataFrame(rows).select(["dataset_id", "symbol", "freq", "as_of", *fields]).lazy()


def _label_fields(labels: list[LabelValue]) -> list[str]:
    return sorted({label.label_id for label in labels})


def _label_rows(labels: list[LabelValue], fields: list[str]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for label in labels:
        key = (label.dataset_id, label.symbol, label.freq, label.as_of)
        row = grouped.setdefault(
            key,
            {
                "dataset_id": label.dataset_id,
                "symbol": label.symbol,
                "freq": label.freq,
                "as_of": label.as_of,
            },
        )
        if label.label_id in fields:
            row[label.label_id] = label.value

    return [
        {**row, **{field: row.get(field) for field in fields}}
        for _, row in sorted(grouped.items())
    ]
