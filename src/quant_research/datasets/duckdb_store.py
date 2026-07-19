from __future__ import annotations

import json
from pathlib import Path

import duckdb

from quant_research.datasets.contracts import (
    MaterializedDatasetCommitResult,
    MaterializedDatasetStatus,
    MaterializedTrainingDatasetManifest,
    TrainingDatasetCommitResult,
    TrainingDatasetError,
    TrainingDatasetManifest,
    TrainingDatasetStatus,
)


_COLUMNS = (
    "training_dataset_id",
    "feature_ref",
    "label_ref",
    "factor_run_id",
    "label_run_id",
    "feature_set_id",
    "label_set_id",
    "dataset_id",
    "freq",
    "feature_fields_json",
    "label_fields_json",
    "row_count_feature",
    "row_count_label",
    "row_count_joined",
    "row_count_feature_only",
    "row_count_label_only",
    "content_hash",
    "status",
    "created_at",
    "market_data_definition_hash",
    "feature_market_data_snapshot_set_hash",
    "label_market_data_snapshot_set_hash",
    "universe_ref",
    "universe_id",
    "universe_version",
    "universe_definition_hash",
    "universe_snapshot_set_hash",
    "label_source_kind",
    "label_source_ref",
    "label_forward_bars",
)

_MATERIALIZED_COLUMNS = (
    "materialized_dataset_id",
    "training_dataset_id",
    "source_content_hash",
    "artifact_ref",
    "artifact_format",
    "feature_fields_json",
    "label_fields_json",
    "schema_fields_json",
    "row_count_input",
    "row_count_materialized",
    "row_count_dropped_warmup",
    "row_count_dropped_null_labels",
    "drop_incomplete_warmup",
    "drop_null_labels",
    "definition_hash",
    "schema_hash",
    "content_hash",
    "artifact_hash",
    "status",
    "created_at",
)


class LocalDuckDBTrainingDatasetStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_manifest(self, manifest: TrainingDatasetManifest) -> TrainingDatasetCommitResult:
        existing = self.get_manifest(manifest.training_dataset_id)
        if existing is not None:
            if existing.content_hash == manifest.content_hash:
                return TrainingDatasetCommitResult(existing, reused_existing=True)
            raise TrainingDatasetError(
                "TRAINING_DATASET_CONFLICT",
                "training_dataset_id already exists with different content or lineage",
            )
        placeholders = ", ".join(["?"] * len(_COLUMNS))
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO training_dataset_manifest ({", ".join(_COLUMNS)})
                VALUES ({placeholders})
                """,
                self._to_row(manifest),
            )
        return TrainingDatasetCommitResult(manifest)

    def get_manifest(self, training_dataset_id: str) -> TrainingDatasetManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {", ".join(_COLUMNS)}
                FROM training_dataset_manifest
                WHERE training_dataset_id = ?
                """,
                [training_dataset_id],
            ).fetchone()
        return self._from_row(row) if row else None

    def commit_materialized_manifest(
        self,
        manifest: MaterializedTrainingDatasetManifest,
    ) -> MaterializedDatasetCommitResult:
        existing = self.get_materialized_manifest(manifest.materialized_dataset_id)
        if existing is not None:
            if (
                existing.definition_hash != manifest.definition_hash
                or existing.content_hash != manifest.content_hash
            ):
                raise TrainingDatasetError(
                    "MATERIALIZED_DATASET_CONFLICT",
                    "materialized_dataset_id already exists with different content or definition",
                )
            if not existing.artifact_path.is_file():
                raise TrainingDatasetError(
                    "MATERIALIZED_ARTIFACT_MISSING",
                    f"committed artifact is missing: {existing.artifact_path}",
                )
            return MaterializedDatasetCommitResult(existing, reused_existing=True)

        placeholders = ", ".join(["?"] * len(_MATERIALIZED_COLUMNS))
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO materialized_training_dataset_manifest (
                    {", ".join(_MATERIALIZED_COLUMNS)}
                )
                VALUES ({placeholders})
                """,
                self._materialized_to_row(manifest),
            )
        return MaterializedDatasetCommitResult(manifest)

    def get_materialized_manifest(
        self,
        materialized_dataset_id: str,
    ) -> MaterializedTrainingDatasetManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {", ".join(_MATERIALIZED_COLUMNS)}
                FROM materialized_training_dataset_manifest
                WHERE materialized_dataset_id = ?
                """,
                [materialized_dataset_id],
            ).fetchone()
        return self._materialized_from_row(row) if row else None

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS training_dataset_manifest (
                    training_dataset_id VARCHAR PRIMARY KEY,
                    feature_ref VARCHAR NOT NULL,
                    label_ref VARCHAR NOT NULL,
                    factor_run_id VARCHAR NOT NULL,
                    label_run_id VARCHAR NOT NULL,
                    feature_set_id VARCHAR NOT NULL,
                    label_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    feature_fields_json VARCHAR NOT NULL,
                    label_fields_json VARCHAR NOT NULL,
                    row_count_feature BIGINT NOT NULL,
                    row_count_label BIGINT NOT NULL,
                    row_count_joined BIGINT NOT NULL,
                    row_count_feature_only BIGINT NOT NULL,
                    row_count_label_only BIGINT NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    market_data_definition_hash VARCHAR,
                    feature_market_data_snapshot_set_hash VARCHAR,
                    label_market_data_snapshot_set_hash VARCHAR,
                    universe_ref VARCHAR,
                    universe_id VARCHAR,
                    universe_version VARCHAR,
                    universe_definition_hash VARCHAR,
                    universe_snapshot_set_hash VARCHAR,
                    label_source_kind VARCHAR NOT NULL,
                    label_source_ref VARCHAR,
                    label_forward_bars BIGINT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS materialized_training_dataset_manifest (
                    materialized_dataset_id VARCHAR PRIMARY KEY,
                    training_dataset_id VARCHAR NOT NULL,
                    source_content_hash VARCHAR NOT NULL,
                    artifact_ref VARCHAR NOT NULL,
                    artifact_format VARCHAR NOT NULL,
                    feature_fields_json VARCHAR NOT NULL,
                    label_fields_json VARCHAR NOT NULL,
                    schema_fields_json VARCHAR NOT NULL,
                    row_count_input BIGINT NOT NULL,
                    row_count_materialized BIGINT NOT NULL,
                    row_count_dropped_warmup BIGINT NOT NULL,
                    row_count_dropped_null_labels BIGINT NOT NULL,
                    drop_incomplete_warmup BOOLEAN NOT NULL,
                    drop_null_labels BOOLEAN NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    schema_hash VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    artifact_hash VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _to_row(self, manifest: TrainingDatasetManifest) -> tuple[object, ...]:
        return (
            manifest.training_dataset_id,
            manifest.feature_ref,
            manifest.label_ref,
            manifest.factor_run_id,
            manifest.label_run_id,
            manifest.feature_set_id,
            manifest.label_set_id,
            manifest.dataset_id,
            manifest.freq,
            json.dumps(list(manifest.feature_fields), sort_keys=True),
            json.dumps(list(manifest.label_fields), sort_keys=True),
            manifest.row_count_feature,
            manifest.row_count_label,
            manifest.row_count_joined,
            manifest.row_count_feature_only,
            manifest.row_count_label_only,
            manifest.content_hash,
            manifest.status.value,
            manifest.created_at,
            manifest.market_data_definition_hash,
            manifest.feature_market_data_snapshot_set_hash,
            manifest.label_market_data_snapshot_set_hash,
            manifest.universe_ref,
            manifest.universe_id,
            manifest.universe_version,
            manifest.universe_definition_hash,
            manifest.universe_snapshot_set_hash,
            manifest.label_source_kind,
            manifest.label_source_ref,
            manifest.label_forward_bars,
        )

    def _from_row(self, row) -> TrainingDatasetManifest:
        return TrainingDatasetManifest(
            training_dataset_id=row[0],
            feature_ref=row[1],
            label_ref=row[2],
            factor_run_id=row[3],
            label_run_id=row[4],
            feature_set_id=row[5],
            label_set_id=row[6],
            dataset_id=row[7],
            freq=row[8],
            feature_fields=tuple(json.loads(row[9])),
            label_fields=tuple(json.loads(row[10])),
            row_count_feature=row[11],
            row_count_label=row[12],
            row_count_joined=row[13],
            row_count_feature_only=row[14],
            row_count_label_only=row[15],
            content_hash=row[16],
            status=TrainingDatasetStatus(row[17]),
            created_at=row[18],
            market_data_definition_hash=row[19],
            feature_market_data_snapshot_set_hash=row[20],
            label_market_data_snapshot_set_hash=row[21],
            universe_ref=row[22],
            universe_id=row[23],
            universe_version=row[24],
            universe_definition_hash=row[25],
            universe_snapshot_set_hash=row[26],
            label_source_kind=row[27],
            label_source_ref=row[28],
            label_forward_bars=row[29],
        )

    def _materialized_to_row(
        self,
        manifest: MaterializedTrainingDatasetManifest,
    ) -> tuple[object, ...]:
        return (
            manifest.materialized_dataset_id,
            manifest.training_dataset_id,
            manifest.source_content_hash,
            manifest.artifact_ref,
            manifest.artifact_format,
            json.dumps(list(manifest.feature_fields), sort_keys=True),
            json.dumps(list(manifest.label_fields), sort_keys=True),
            json.dumps(list(manifest.schema_fields)),
            manifest.row_count_input,
            manifest.row_count_materialized,
            manifest.row_count_dropped_warmup,
            manifest.row_count_dropped_null_labels,
            manifest.drop_incomplete_warmup,
            manifest.drop_null_labels,
            manifest.definition_hash,
            manifest.schema_hash,
            manifest.content_hash,
            manifest.artifact_hash,
            manifest.status.value,
            manifest.created_at,
        )

    def _materialized_from_row(self, row) -> MaterializedTrainingDatasetManifest:
        return MaterializedTrainingDatasetManifest(
            materialized_dataset_id=row[0],
            training_dataset_id=row[1],
            source_content_hash=row[2],
            artifact_ref=row[3],
            artifact_format=row[4],
            feature_fields=tuple(json.loads(row[5])),
            label_fields=tuple(json.loads(row[6])),
            schema_fields=tuple(json.loads(row[7])),
            row_count_input=row[8],
            row_count_materialized=row[9],
            row_count_dropped_warmup=row[10],
            row_count_dropped_null_labels=row[11],
            drop_incomplete_warmup=row[12],
            drop_null_labels=row[13],
            definition_hash=row[14],
            schema_hash=row[15],
            content_hash=row[16],
            artifact_hash=row[17],
            status=MaterializedDatasetStatus(row[18]),
            created_at=row[19],
        )
