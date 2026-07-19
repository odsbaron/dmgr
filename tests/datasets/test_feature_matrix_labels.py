from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from quant_research.contracts.bar import Frequency
from quant_research.datasets import TrainingFeatureMatrixBuilder
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.gates import FeatureQualityGate
from quant_research.features.quality import (
    FactorQualityMetric,
    FactorQualityReport,
    QualitySeverity,
    QualityStatus,
)
from quant_research.labels.contracts import LabelCommitRequest, LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.gates import LabelConsumptionBlocked, LabelQualityGate
from quant_research.labels.quality import LabelQualityAnalyzer


def factor_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="price_features",
        version="1.0.0",
        namespace="price",
        description="Price feature test bundle.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=2,
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
    return pl.DataFrame(
        [
            {
                "dataset_id": "fixture-daily",
                "symbol": "000001.SZ",
                "freq": "1d",
                "as_of": start + timedelta(days=index),
                "ret_1": value,
            }
            for index, value in enumerate([None, 0.01])
        ]
    ).lazy()


def commit_feature_run(store: LocalDuckDBFeatureStore):
    return store.commit_run(
        FeatureCommitRequest(
            config=run_config(),
            factor_frame=factor_frame(),
            resolved_factors=(RegisteredFactor(factor_spec(), compute=None),),
            input_row_count=2,
        )
    )


def quality_report() -> FactorQualityReport:
    return FactorQualityReport(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        status=QualityStatus.PASSED,
        metrics=(
            FactorQualityMetric(
                factor_run_id="factor-run-1",
                feature_set_id="basic_price_v1",
                factor_id="price_features",
                output_field="ret_1",
                metric_name="null_ratio",
                metric_value=0.0,
                metric_json={},
                severity=QualitySeverity.INFO,
                created_at="2026-07-08T00:00:00+00:00",
            ),
        ),
    )


def label_value(index: int, value_float: float | None) -> LabelValue:
    return LabelValue(
        label_run_id="label-run-1",
        label_set_id="next_return_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        label_id="forward_ret_1",
        label_version="1.0.0",
        value_float=value_float,
        value_string=None,
        value_kind="null" if value_float is None else "float",
        forward_bars=1,
        source_factor_run_id="factor-run-forward",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_training_feature_matrix_builder_joins_quality_gated_features_with_labels(tmp_path):
    db_path = tmp_path / "research.duckdb"
    feature_store = LocalDuckDBFeatureStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    feature_commit = commit_feature_run(feature_store)
    feature_store.commit_quality_report(quality_report())
    label_ref = label_store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="factor-run-forward",
            labels=(label_value(0, 0.02), label_value(1, None)),
        )
    )
    label_report = LabelQualityAnalyzer(max_null_ratio=0.6).analyze(
        tuple(label_store.read_labels(label_ref))
    )
    label_store.commit_quality_report(label_report)

    frame = TrainingFeatureMatrixBuilder(
        FeatureQualityGate(feature_store),
        label_gate=LabelQualityGate(label_store),
    ).build_with_labels(
        feature_commit.snapshot_ref,
        label_ref,
        feature_fields=("ret_1",),
        label_fields=("forward_ret_1",),
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
        "forward_ret_1",
    ]
    assert collected["ret_1"].to_list() == [None, 0.01]
    assert collected["forward_ret_1"].to_list() == [0.02, None]


def test_training_feature_matrix_builder_blocks_failed_label_quality(tmp_path):
    db_path = tmp_path / "research.duckdb"
    feature_store = LocalDuckDBFeatureStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    feature_commit = commit_feature_run(feature_store)
    feature_store.commit_quality_report(quality_report())
    label_ref = label_store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="factor-run-forward",
            labels=(label_value(0, 0.02), label_value(1, None)),
        )
    )
    label_report = LabelQualityAnalyzer(max_null_ratio=0.1).analyze(
        tuple(label_store.read_labels(label_ref))
    )
    label_store.commit_quality_report(label_report)

    with pytest.raises(LabelConsumptionBlocked):
        TrainingFeatureMatrixBuilder(
            FeatureQualityGate(feature_store),
            label_gate=LabelQualityGate(label_store),
        ).build_with_labels(feature_commit.snapshot_ref, label_ref)
