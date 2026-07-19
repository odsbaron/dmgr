from dataclasses import replace

import pytest

from quant_research.datasets import (
    LocalDuckDBTrainingDatasetStore,
    TrainingDatasetError,
    TrainingFeatureMatrixBuilder,
)
from quant_research.features.gates import FeatureQualityGate
from quant_research.labels.contracts import LabelCommitRequest, LabelSourceKind
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.gates import LabelQualityGate
from quant_research.labels.quality import LabelQualityAnalyzer

from test_feature_matrix_labels import (
    commit_feature_run,
    label_value,
    quality_report,
)


def _commit_labels(
    store: LocalDuckDBLabelStore,
    *,
    definition_hash: str,
    labels=None,
    source_as_of_end: str = "2026-07-03T07:00:00+00:00",
):
    values = labels or (label_value(0, 0.02), label_value(1, 0.03))
    ref = store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="duckdb://curated_market_bar?snapshot_set_id=label-set",
            labels=values,
            source_kind=LabelSourceKind.MARKET_DATA,
            source_ref="duckdb://curated_market_bar?snapshot_set_id=label-set",
            dataset_id="fixture-daily",
            freq="1d",
            forward_bars=1,
            source_as_of_start="2026-07-01T07:00:00+00:00",
            source_as_of_end=source_as_of_end,
            market_data_ref="duckdb://curated_market_bar?snapshot_set_id=label-set",
            market_dataset_version="v1",
            market_data_definition_hash=definition_hash,
            market_data_snapshot_set_hash="sha256:label-set",
        )
    )
    report = LabelQualityAnalyzer(max_null_ratio=1.0).analyze(tuple(store.read_labels(ref)))
    store.commit_quality_report(report)
    return ref


def _stores(tmp_path):
    from quant_research.features.duckdb_store import LocalDuckDBFeatureStore

    db_path = tmp_path / "research.duckdb"
    feature_store = LocalDuckDBFeatureStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    dataset_store = LocalDuckDBTrainingDatasetStore(db_path)
    feature_commit = commit_feature_run(feature_store)
    feature_store.commit_quality_report(quality_report())
    return feature_store, label_store, dataset_store, feature_commit


def test_builder_rejects_different_exact_market_data_definitions(tmp_path):
    feature_store, label_store, _, feature_commit = _stores(tmp_path)
    label_ref = _commit_labels(label_store, definition_hash="sha256:label-definition")
    with feature_store._connect() as conn:
        conn.execute(
            """
            UPDATE factor_run_manifest
            SET market_data_ref = ?, market_dataset_version = ?,
                market_data_definition_hash = ?, market_data_snapshot_set_hash = ?
            WHERE factor_run_id = ?
            """,
            ["feature-ref", "v1", "sha256:feature-definition", "sha256:feature-set", "factor-run-1"],
        )

    with pytest.raises(TrainingDatasetError) as exc_info:
        TrainingFeatureMatrixBuilder(
            FeatureQualityGate(feature_store),
            LabelQualityGate(label_store),
        ).build_with_labels(feature_commit.snapshot_ref, label_ref)

    assert exc_info.value.code == "MARKET_DATA_DEFINITION_MISMATCH"


def test_builder_rejects_exact_label_source_without_forward_extension(tmp_path):
    feature_store, label_store, _, feature_commit = _stores(tmp_path)
    label_ref = _commit_labels(
        label_store,
        definition_hash="sha256:shared-definition",
        source_as_of_end="2026-07-02T07:00:00+00:00",
    )

    with pytest.raises(TrainingDatasetError) as exc_info:
        TrainingFeatureMatrixBuilder(
            FeatureQualityGate(feature_store),
            LabelQualityGate(label_store),
        ).build_with_labels(feature_commit.snapshot_ref, label_ref)

    assert exc_info.value.code == "LABEL_FORWARD_HORIZON_NOT_COVERED"


def test_manifested_assembly_counts_wider_labels_and_is_idempotent(tmp_path):
    feature_store, label_store, dataset_store, feature_commit = _stores(tmp_path)
    extra = replace(label_value(0, 0.04), symbol="000002.SZ")
    label_ref = _commit_labels(
        label_store,
        definition_hash="sha256:shared-definition",
        labels=(label_value(0, 0.02), label_value(1, 0.03), extra),
    )
    with feature_store._connect() as conn:
        conn.execute(
            """
            UPDATE factor_run_manifest
            SET market_data_ref = ?, market_dataset_version = ?,
                market_data_definition_hash = ?, market_data_snapshot_set_hash = ?
            WHERE factor_run_id = ?
            """,
            ["feature-ref", "v1", "sha256:shared-definition", "sha256:feature-set", "factor-run-1"],
        )
    builder = TrainingFeatureMatrixBuilder(
        FeatureQualityGate(feature_store),
        LabelQualityGate(label_store),
        dataset_store,
    )

    first = builder.build_manifested(
        "training-v1",
        feature_commit.snapshot_ref,
        label_ref,
        feature_fields=("ret_1",),
        label_fields=("forward_ret_1",),
    )
    second = builder.build_manifested(
        "training-v1",
        feature_commit.snapshot_ref,
        label_ref,
        feature_fields=("ret_1",),
        label_fields=("forward_ret_1",),
    )

    assert first.matrix.collect().height == 2
    assert first.manifest.row_count_feature == 2
    assert first.manifest.row_count_label == 3
    assert first.manifest.row_count_joined == 2
    assert first.manifest.row_count_feature_only == 0
    assert first.manifest.row_count_label_only == 1
    assert first.manifest.feature_market_data_snapshot_set_hash == "sha256:feature-set"
    assert first.manifest.label_market_data_snapshot_set_hash == "sha256:label-set"
    assert second.reused_existing is True


def test_training_dataset_id_conflict_preserves_original_manifest(tmp_path):
    _, _, dataset_store, _ = _stores(tmp_path)
    from quant_research.datasets.contracts import TrainingDatasetManifest, TrainingDatasetStatus

    manifest = TrainingDatasetManifest(
        training_dataset_id="training-v1",
        feature_ref="feature-ref",
        label_ref="label-ref",
        factor_run_id="factor-run",
        label_run_id="label-run",
        feature_set_id="features",
        label_set_id="labels",
        dataset_id="dataset",
        freq="1m",
        feature_fields=("x",),
        label_fields=("y",),
        row_count_feature=1,
        row_count_label=1,
        row_count_joined=1,
        row_count_feature_only=0,
        row_count_label_only=0,
        content_hash="sha256:first",
        status=TrainingDatasetStatus.COMMITTED,
        created_at="2026-07-18T00:00:00+00:00",
    )
    dataset_store.commit_manifest(manifest)

    with pytest.raises(TrainingDatasetError) as exc_info:
        dataset_store.commit_manifest(replace(manifest, content_hash="sha256:second"))

    assert exc_info.value.code == "TRAINING_DATASET_CONFLICT"
    assert dataset_store.get_manifest("training-v1").content_hash == "sha256:first"
