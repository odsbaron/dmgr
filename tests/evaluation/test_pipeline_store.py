from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import polars as pl
import pytest

from quant_research.contracts.bar import Frequency
from quant_research.evaluation import (
    FactorEvaluationError,
    FactorEvaluationPipeline,
    FactorEvaluationRequest,
    LocalDuckDBEvaluationStore,
)
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.gates import FeatureConsumptionBlocked, FeatureQualityGate
from quant_research.features.quality import FactorQualityAnalyzer
from quant_research.labels.contracts import LabelCommitRequest, LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.gates import LabelConsumptionBlocked, LabelQualityGate
from quant_research.labels.quality import LabelQualityAnalyzer


AS_OF = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)


def _factor_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="alpha",
        version="1.0.0",
        namespace="test",
        description="Synthetic alpha.",
        input_fields=("close",),
        output_fields=("alpha",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.POLARS_EXPR,
    )


def _seed_inputs(
    tmp_path,
    *,
    feature_quality: bool = True,
    label_quality: bool = True,
    label_dataset_id: str = "fixture-daily",
):
    db_path = tmp_path / "research.duckdb"
    feature_store = LocalDuckDBFeatureStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    evaluation_store = LocalDuckDBEvaluationStore(db_path)
    registered = RegisteredFactor(_factor_spec(), compute=None)
    rows = [
        {
            "dataset_id": "fixture-daily",
            "symbol": f"{index:06d}.SZ",
            "freq": "1d",
            "as_of": AS_OF,
            "alpha": float(index),
        }
        for index in range(6)
    ]
    feature_commit = feature_store.commit_run(
        FeatureCommitRequest(
            config=FactorRunConfig(
                factor_run_id="factor-run-1",
                feature_set_id="alpha-v1",
                input_data_ref=("duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d"),
                factor_ids=("alpha",),
                freq=Frequency.D1,
                dataset_id="fixture-daily",
            ),
            factor_frame=pl.DataFrame(rows).lazy(),
            resolved_factors=(registered,),
            input_row_count=len(rows),
        )
    )
    if feature_quality:
        feature_values = feature_store.read_feature_table(feature_commit.feature_table_ref)
        feature_store.commit_quality_report(
            FactorQualityAnalyzer().analyze(feature_values, (registered,))
        )

    label_values = tuple(
        LabelValue(
            label_run_id="label-run-1",
            label_set_id="forward-v1",
            dataset_id=label_dataset_id,
            symbol=f"{index:06d}.SZ",
            freq="1d",
            as_of=AS_OF.isoformat(),
            label_id="forward_ret_1",
            label_version="1.0.0",
            value_float=float(index),
            value_string=None,
            value_kind="float",
            forward_bars=1,
            source_factor_run_id="factor-run-1",
            created_at="2026-07-02T00:00:00+00:00",
        )
        for index in range(6)
    )
    label_ref = label_store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="forward-v1",
            source_factor_run_id="factor-run-1",
            labels=label_values,
            dataset_id=label_dataset_id,
            freq="1d",
            forward_bars=1,
        )
    )
    if label_quality:
        label_store.commit_quality_report(
            LabelQualityAnalyzer(max_null_ratio=0.0).analyze(label_values)
        )

    pipeline = FactorEvaluationPipeline(
        FeatureQualityGate(feature_store),
        LabelQualityGate(label_store),
        evaluation_store,
    )
    request = FactorEvaluationRequest(
        evaluation_run_id="evaluation-run-1",
        feature_snapshot_ref=feature_commit.snapshot_ref.uri,
        label_ref=label_ref.uri,
        factor_fields=("alpha",),
        label_field="forward_ret_1",
        quantile_count=3,
        minimum_cross_section_size=4,
    )
    return pipeline, evaluation_store, request


def test_pipeline_persists_lineage_metrics_and_stable_refs(tmp_path):
    pipeline, store, request = _seed_inputs(tmp_path)

    result = pipeline.run(request)

    manifest = store.get_manifest(request.evaluation_run_id)
    metrics = store.read_metrics(result.metric_ref)
    assert result.manifest_ref.uri == (
        "duckdb://factor_evaluation_manifest?evaluation_run_id=evaluation-run-1"
    )
    assert result.metric_ref.uri == (
        "duckdb://factor_evaluation_metric?evaluation_run_id=evaluation-run-1"
    )
    assert manifest.factor_run_id == "factor-run-1"
    assert manifest.label_run_id == "label-run-1"
    assert manifest.metric_ref == result.metric_ref.uri
    assert manifest.metric_count == len(metrics) == 6
    assert manifest.config_hash.startswith("sha256:")
    assert manifest.content_hash.startswith("sha256:")


@pytest.mark.parametrize(
    ("feature_quality", "label_quality", "error_type"),
    [
        (False, True, FeatureConsumptionBlocked),
        (True, False, LabelConsumptionBlocked),
    ],
)
def test_pipeline_blocks_non_passed_inputs_without_persisting(
    tmp_path,
    feature_quality,
    label_quality,
    error_type,
):
    pipeline, store, request = _seed_inputs(
        tmp_path,
        feature_quality=feature_quality,
        label_quality=label_quality,
    )

    with pytest.raises(error_type):
        pipeline.run(request)

    assert store.get_manifest(request.evaluation_run_id) is None


def test_pipeline_rejects_incompatible_dataset_lineage(tmp_path):
    pipeline, store, request = _seed_inputs(tmp_path, label_dataset_id="other-dataset")

    with pytest.raises(FactorEvaluationError) as exc_info:
        pipeline.run(request)

    assert exc_info.value.code == "DATASET_ID_MISMATCH"
    assert store.get_manifest(request.evaluation_run_id) is None


def test_identical_run_is_reused_and_conflicting_run_is_rejected(tmp_path):
    pipeline, store, request = _seed_inputs(tmp_path)

    first = pipeline.run(request)
    second = pipeline.run(request)

    assert first.reused_existing is False
    assert second.reused_existing is True
    original = store.get_manifest(request.evaluation_run_id)

    with pytest.raises(FactorEvaluationError) as exc_info:
        pipeline.run(replace(request, quantile_count=2))

    assert exc_info.value.code == "EVALUATION_RUN_CONFLICT"
    assert store.get_manifest(request.evaluation_run_id) == original


def test_metric_write_failure_rolls_back_manifest_and_rows(tmp_path, monkeypatch):
    pipeline, store, request = _seed_inputs(tmp_path)

    def fail_metric_write(_conn, _metrics):
        raise RuntimeError("synthetic metric failure")

    monkeypatch.setattr(store, "_insert_metrics", fail_metric_write)

    with pytest.raises(RuntimeError, match="synthetic metric failure"):
        pipeline.run(request)

    assert store.get_manifest(request.evaluation_run_id) is None
    assert (
        store.read_metrics("duckdb://factor_evaluation_metric?evaluation_run_id=evaluation-run-1")
        == []
    )
