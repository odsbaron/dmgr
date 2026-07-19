from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from quant_research.datasets.contracts import (
    MaterializedDatasetStatus,
    MaterializedTrainingDatasetManifest,
    MaterializedTrainingDatasetResult,
    MaterializeTrainingDatasetRequest,
    TrainingDatasetError,
    parquet_artifact_ref,
    training_dataset_hash,
)
from quant_research.datasets.duckdb_store import LocalDuckDBTrainingDatasetStore

if TYPE_CHECKING:
    from quant_research.datasets.feature_matrix import TrainingDatasetBuildResult


_SORT_COLUMNS = ("dataset_id", "symbol", "freq", "as_of")


@dataclass(frozen=True)
class TrainingDatasetMaterializer:
    dataset_store: LocalDuckDBTrainingDatasetStore

    def materialize(
        self,
        build: TrainingDatasetBuildResult,
        request: MaterializeTrainingDatasetRequest,
    ) -> MaterializedTrainingDatasetResult:
        frame = build.matrix.collect()
        if frame.height != build.manifest.row_count_joined:
            raise TrainingDatasetError(
                "MATERIALIZATION_SOURCE_COUNT_MISMATCH",
                "assembled matrix row count differs from its training-dataset manifest",
            )
        _assert_required_fields(frame, build.manifest.label_fields, request)
        filtered, dropped_warmup, dropped_null_labels = _apply_filters(
            frame,
            label_fields=build.manifest.label_fields,
            request=request,
        )
        sorted_frame = _sort_frame(filtered)
        schema_payload = [
            {"name": name, "dtype": str(dtype)} for name, dtype in sorted_frame.schema.items()
        ]
        schema_hash = training_dataset_hash(schema_payload)
        content_hash = training_dataset_hash(
            {
                "schema": schema_payload,
                "rows": sorted_frame.to_dicts(),
            }
        )
        definition_hash = training_dataset_hash(
            {
                "materialized_dataset_id": request.materialized_dataset_id,
                "training_dataset_id": build.manifest.training_dataset_id,
                "source_content_hash": build.manifest.content_hash,
                "feature_fields": list(build.manifest.feature_fields),
                "label_fields": list(build.manifest.label_fields),
                "drop_incomplete_warmup": request.drop_incomplete_warmup,
                "drop_null_labels": request.drop_null_labels,
                "output_dir": str(request.resolved_output_dir),
            }
        )
        target = _artifact_path(request, content_hash)
        existing = self.dataset_store.get_materialized_manifest(request.materialized_dataset_id)
        if existing is not None:
            proposed = _manifest(
                build=build,
                request=request,
                frame=sorted_frame,
                target=target,
                dropped_warmup=dropped_warmup,
                dropped_null_labels=dropped_null_labels,
                definition_hash=definition_hash,
                schema_hash=schema_hash,
                content_hash=content_hash,
                artifact_hash=existing.artifact_hash,
                created_at=existing.created_at,
            )
            commit = self.dataset_store.commit_materialized_manifest(proposed)
            if _file_hash(commit.manifest.artifact_path) != commit.manifest.artifact_hash:
                raise TrainingDatasetError(
                    "MATERIALIZED_ARTIFACT_HASH_MISMATCH",
                    "committed artifact bytes do not match its manifest",
                )
            return MaterializedTrainingDatasetResult(
                manifest=commit.manifest,
                reused_existing=commit.reused_existing,
            )

        request.resolved_output_dir.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise TrainingDatasetError(
                "MATERIALIZED_ARTIFACT_CONFLICT",
                f"artifact path already exists and will not be overwritten: {target}",
            )

        artifact_hash = _publish_parquet(sorted_frame, target)
        manifest = _manifest(
            build=build,
            request=request,
            frame=sorted_frame,
            target=target,
            dropped_warmup=dropped_warmup,
            dropped_null_labels=dropped_null_labels,
            definition_hash=definition_hash,
            schema_hash=schema_hash,
            content_hash=content_hash,
            artifact_hash=artifact_hash,
            created_at=datetime.now(UTC).isoformat(),
        )
        try:
            commit = self.dataset_store.commit_materialized_manifest(manifest)
        except Exception:
            target.unlink(missing_ok=True)
            raise
        return MaterializedTrainingDatasetResult(
            manifest=commit.manifest,
            reused_existing=commit.reused_existing,
        )


def _assert_required_fields(
    frame: pl.DataFrame,
    label_fields: tuple[str, ...],
    request: MaterializeTrainingDatasetRequest,
) -> None:
    missing = [field for field in label_fields if field not in frame.columns]
    if missing:
        raise TrainingDatasetError(
            "MATERIALIZATION_SCHEMA_MISMATCH",
            f"assembled matrix is missing label fields: {', '.join(missing)}",
        )
    if request.drop_incomplete_warmup and "warmup_complete" not in frame.columns:
        raise TrainingDatasetError(
            "MATERIALIZATION_SCHEMA_MISMATCH",
            "assembled matrix is missing warmup_complete",
        )


def _apply_filters(
    frame: pl.DataFrame,
    *,
    label_fields: tuple[str, ...],
    request: MaterializeTrainingDatasetRequest,
) -> tuple[pl.DataFrame, int, int]:
    filtered = frame
    dropped_warmup = 0
    if request.drop_incomplete_warmup:
        warmup_filter = pl.col("warmup_complete").fill_null(False)
        after_warmup = filtered.filter(warmup_filter)
        dropped_warmup = filtered.height - after_warmup.height
        filtered = after_warmup

    dropped_null_labels = 0
    if request.drop_null_labels and label_fields:
        label_filter = pl.all_horizontal(*(pl.col(field).is_not_null() for field in label_fields))
        after_labels = filtered.filter(label_filter)
        dropped_null_labels = filtered.height - after_labels.height
        filtered = after_labels
    return filtered, dropped_warmup, dropped_null_labels


def _sort_frame(frame: pl.DataFrame) -> pl.DataFrame:
    sort_columns = [column for column in _SORT_COLUMNS if column in frame.columns]
    return frame.sort(sort_columns) if sort_columns else frame


def _artifact_path(
    request: MaterializeTrainingDatasetRequest,
    content_hash: str,
) -> Path:
    digest = content_hash.removeprefix("sha256:")[:16]
    return request.resolved_output_dir / f"{request.materialized_dataset_id}-{digest}.parquet"


def _publish_parquet(frame: pl.DataFrame, target: Path) -> str:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{target.stem}-",
            suffix=".tmp",
            dir=target.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        frame.write_parquet(temporary_path, compression="zstd", statistics=True)
        artifact_hash = _file_hash(temporary_path)
        try:
            os.link(temporary_path, target)
        except FileExistsError as exc:
            raise TrainingDatasetError(
                "MATERIALIZED_ARTIFACT_CONFLICT",
                f"artifact path already exists and will not be overwritten: {target}",
            ) from exc
        return artifact_hash
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _manifest(
    *,
    build: TrainingDatasetBuildResult,
    request: MaterializeTrainingDatasetRequest,
    frame: pl.DataFrame,
    target: Path,
    dropped_warmup: int,
    dropped_null_labels: int,
    definition_hash: str,
    schema_hash: str,
    content_hash: str,
    artifact_hash: str,
    created_at: str,
) -> MaterializedTrainingDatasetManifest:
    return MaterializedTrainingDatasetManifest(
        materialized_dataset_id=request.materialized_dataset_id,
        training_dataset_id=build.manifest.training_dataset_id,
        source_content_hash=build.manifest.content_hash,
        artifact_ref=parquet_artifact_ref(target),
        artifact_format="parquet",
        feature_fields=build.manifest.feature_fields,
        label_fields=build.manifest.label_fields,
        schema_fields=tuple(frame.columns),
        row_count_input=build.manifest.row_count_joined,
        row_count_materialized=frame.height,
        row_count_dropped_warmup=dropped_warmup,
        row_count_dropped_null_labels=dropped_null_labels,
        drop_incomplete_warmup=request.drop_incomplete_warmup,
        drop_null_labels=request.drop_null_labels,
        definition_hash=definition_hash,
        schema_hash=schema_hash,
        content_hash=content_hash,
        artifact_hash=artifact_hash,
        status=MaterializedDatasetStatus.COMMITTED,
        created_at=created_at,
    )
