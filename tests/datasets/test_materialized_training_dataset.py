from __future__ import annotations

from dataclasses import replace

import polars as pl
import pytest

from quant_research.datasets.contracts import (
    MaterializeTrainingDatasetRequest,
    TrainingDatasetError,
    TrainingDatasetManifest,
    TrainingDatasetStatus,
)
from quant_research.datasets.duckdb_store import LocalDuckDBTrainingDatasetStore
from quant_research.datasets.feature_matrix import TrainingDatasetBuildResult
from quant_research.datasets.materialization import TrainingDatasetMaterializer


def _manifest(*, content_hash: str = "sha256:source") -> TrainingDatasetManifest:
    return TrainingDatasetManifest(
        training_dataset_id="training-v1",
        feature_ref="duckdb://feature_snapshot?factor_run_id=factor-run",
        label_ref="duckdb://label_table?label_run_id=label-run",
        factor_run_id="factor-run",
        label_run_id="label-run",
        feature_set_id="features-v1",
        label_set_id="labels-v1",
        dataset_id="fixture-daily",
        freq="1d",
        feature_fields=("factor",),
        label_fields=("target",),
        row_count_feature=3,
        row_count_label=3,
        row_count_joined=3,
        row_count_feature_only=0,
        row_count_label_only=0,
        content_hash=content_hash,
        status=TrainingDatasetStatus.COMMITTED,
        created_at="2026-07-19T00:00:00+00:00",
    )


def _build() -> TrainingDatasetBuildResult:
    frame = pl.DataFrame(
        [
            {
                "feature_set_id": "features-v1",
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": "2026-07-01T07:00:00+00:00",
                "warmup_complete": False,
                "feature_ref": "feature-1",
                "factor": None,
                "target": 0.01,
            },
            {
                "feature_set_id": "features-v1",
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": "2026-07-02T07:00:00+00:00",
                "warmup_complete": True,
                "feature_ref": "feature-2",
                "factor": 0.02,
                "target": None,
            },
            {
                "feature_set_id": "features-v1",
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": "2026-07-03T07:00:00+00:00",
                "warmup_complete": True,
                "feature_ref": "feature-3",
                "factor": 0.03,
                "target": 0.04,
            },
        ]
    )
    return TrainingDatasetBuildResult(matrix=frame.lazy(), manifest=_manifest())


def test_materialization_filters_rows_and_reuses_identical_asset(tmp_path):
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    materializer = TrainingDatasetMaterializer(store)
    request = MaterializeTrainingDatasetRequest("materialized-v1", tmp_path / "artifacts")

    first = materializer.materialize(_build(), request)
    second = materializer.materialize(_build(), request)
    persisted = store.get_materialized_manifest("materialized-v1")

    assert first.manifest.artifact_ref.startswith("parquet:///")
    assert first.manifest.artifact_path.is_file()
    assert pl.read_parquet(first.manifest.artifact_path).height == 1
    assert first.manifest.row_count_input == 3
    assert first.manifest.row_count_materialized == 1
    assert first.manifest.row_count_dropped_warmup == 1
    assert first.manifest.row_count_dropped_null_labels == 1
    assert first.manifest.schema_hash.startswith("sha256:")
    assert first.manifest.content_hash.startswith("sha256:")
    assert first.manifest.artifact_hash.startswith("sha256:")
    assert persisted == first.manifest
    assert second.manifest == first.manifest
    assert second.reused_existing is True


def test_materialization_can_preserve_warmup_and_null_label_rows(tmp_path):
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    result = TrainingDatasetMaterializer(store).materialize(
        _build(),
        MaterializeTrainingDatasetRequest(
            "unfiltered-v1",
            tmp_path / "artifacts",
            drop_incomplete_warmup=False,
            drop_null_labels=False,
        ),
    )

    assert pl.read_parquet(result.manifest.artifact_path).height == 3
    assert result.manifest.row_count_dropped_warmup == 0
    assert result.manifest.row_count_dropped_null_labels == 0


def test_materialized_dataset_id_conflict_preserves_original_asset(tmp_path):
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    materializer = TrainingDatasetMaterializer(store)
    request = MaterializeTrainingDatasetRequest("materialized-v1", tmp_path / "artifacts")
    original = materializer.materialize(_build(), request)
    original_bytes = original.manifest.artifact_path.read_bytes()

    conflicting_build = replace(
        _build(),
        matrix=_build().matrix.with_columns(pl.lit(9.0).alias("factor")),
    )
    with pytest.raises(TrainingDatasetError) as exc_info:
        materializer.materialize(conflicting_build, request)

    assert exc_info.value.code == "MATERIALIZED_DATASET_CONFLICT"
    assert store.get_materialized_manifest("materialized-v1") == original.manifest
    assert original.manifest.artifact_path.read_bytes() == original_bytes


def test_existing_artifact_without_manifest_is_not_overwritten(tmp_path):
    output_dir = tmp_path / "artifacts"
    first_store = LocalDuckDBTrainingDatasetStore(tmp_path / "first.duckdb")
    first = TrainingDatasetMaterializer(first_store).materialize(
        _build(),
        MaterializeTrainingDatasetRequest("materialized-v1", output_dir),
    )
    original_bytes = first.manifest.artifact_path.read_bytes()
    second_store = LocalDuckDBTrainingDatasetStore(tmp_path / "second.duckdb")

    with pytest.raises(TrainingDatasetError) as exc_info:
        TrainingDatasetMaterializer(second_store).materialize(
            _build(),
            MaterializeTrainingDatasetRequest("materialized-v1", output_dir),
        )

    assert exc_info.value.code == "MATERIALIZED_ARTIFACT_CONFLICT"
    assert first.manifest.artifact_path.read_bytes() == original_bytes


def test_identical_reuse_fails_when_committed_artifact_is_missing(tmp_path):
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    materializer = TrainingDatasetMaterializer(store)
    request = MaterializeTrainingDatasetRequest("materialized-v1", tmp_path / "artifacts")
    first = materializer.materialize(_build(), request)
    first.manifest.artifact_path.unlink()

    with pytest.raises(TrainingDatasetError) as exc_info:
        materializer.materialize(_build(), request)

    assert exc_info.value.code == "MATERIALIZED_ARTIFACT_MISSING"


def test_identical_reuse_fails_when_committed_artifact_is_tampered(tmp_path):
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    materializer = TrainingDatasetMaterializer(store)
    request = MaterializeTrainingDatasetRequest("materialized-v1", tmp_path / "artifacts")
    first = materializer.materialize(_build(), request)
    first.manifest.artifact_path.write_bytes(b"tampered")

    with pytest.raises(TrainingDatasetError) as exc_info:
        materializer.materialize(_build(), request)

    assert exc_info.value.code == "MATERIALIZED_ARTIFACT_HASH_MISMATCH"
