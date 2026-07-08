from datetime import UTC, datetime, timedelta

import pytest
import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.datasets import TrainingFeatureMatrixBuilder, snapshots_to_feature_matrix
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest
from quant_research.features.contracts import FeatureSnapshot
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.gates import FeatureConsumptionBlocked, FeatureQualityGate
from quant_research.features.quality import (
    FactorQualityMetric,
    FactorQualityReport,
    QualitySeverity,
    QualityStatus,
)


def snapshot(index: int, features: dict[str, object]) -> FeatureSnapshot:
    return FeatureSnapshot(
        snapshot_id=f"snapshot-{index}",
        feature_set_id="basic_price_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        features=features,
        factor_run_ids=("factor-run-1",),
        input_data_refs=("duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",),
        warmup_complete=index > 0,
        quality_flags=(),
        feature_ref=f"duckdb://feature_snapshot?snapshot_id=snapshot-{index}",
        created_at="2026-07-08T00:00:00+00:00",
    )


def factor_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="price_features",
        version="1.0.0",
        namespace="price",
        description="Price feature test bundle.",
        input_fields=("close",),
        output_fields=("ret_1", "ma_3"),
        supported_freqs=(Frequency.D1,),
        lookback_bars=3,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )


def run_config() -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=("price_features",),
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def factor_frame() -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index, values in enumerate([(None, None), (0.01, 10.2)]):
        ret_1, ma_3 = values
        rows.append(
            {
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": start + timedelta(days=index),
                "ret_1": ret_1,
                "ma_3": ma_3,
            }
        )
    return pl.DataFrame(rows).lazy()


def commit_feature_run(store: LocalDuckDBFeatureStore):
    return store.commit_run(
        FeatureCommitRequest(
            config=run_config(),
            factor_frame=factor_frame(),
            resolved_factors=(RegisteredFactor(factor_spec(), compute=None),),
            input_row_count=2,
        )
    )


def quality_report(status: QualityStatus, severity: QualitySeverity) -> FactorQualityReport:
    return FactorQualityReport(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        status=status,
        metrics=(
            FactorQualityMetric(
                factor_run_id="factor-run-1",
                feature_set_id="basic_price_v1",
                factor_id="price_features",
                output_field="ret_1",
                metric_name="null_ratio",
                metric_value=0.0,
                metric_json={},
                severity=severity,
                created_at="2026-07-08T00:00:00+00:00",
            ),
        ),
    )


def test_snapshots_to_feature_matrix_expands_feature_json_with_stable_columns():
    frame = snapshots_to_feature_matrix(
        [
            snapshot(0, {"ret_1": None, "ma_3": None}),
            snapshot(1, {"ret_1": 0.01, "ma_3": 10.2}),
        ]
    ).collect()

    assert frame.columns == [
        "feature_set_id",
        "dataset_id",
        "symbol",
        "freq",
        "as_of",
        "warmup_complete",
        "feature_ref",
        "ma_3",
        "ret_1",
    ]
    assert frame["ret_1"].to_list() == [None, 0.01]
    assert frame["ma_3"].to_list() == [None, 10.2]
    assert frame["warmup_complete"].to_list() == [False, True]


def test_training_feature_matrix_builder_reads_only_consumable_snapshots(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)
    store.commit_quality_report(quality_report(QualityStatus.PASSED, QualitySeverity.INFO))

    frame = TrainingFeatureMatrixBuilder(FeatureQualityGate(store)).build(
        commit.snapshot_ref,
        feature_fields=("ret_1",),
    )

    collected = frame.collect()
    assert collected.columns == [
        "feature_set_id",
        "dataset_id",
        "symbol",
        "freq",
        "as_of",
        "warmup_complete",
        "feature_ref",
        "ret_1",
    ]
    assert collected["ret_1"].to_list() == [None, 0.01]


def test_training_feature_matrix_builder_blocks_failed_quality_snapshot(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)
    store.commit_quality_report(quality_report(QualityStatus.FAILED, QualitySeverity.ERROR))

    with pytest.raises(FeatureConsumptionBlocked):
        TrainingFeatureMatrixBuilder(FeatureQualityGate(store)).build(commit.snapshot_ref)


def test_snapshots_to_feature_matrix_rejects_empty_input():
    with pytest.raises(ValueError, match="snapshots must not be empty"):
        snapshots_to_feature_matrix([])
