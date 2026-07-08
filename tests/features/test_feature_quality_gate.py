from datetime import UTC, datetime, timedelta

import pytest
import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.contracts.refs import DataRef
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest, FeatureRunStatus
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.gates import FeatureConsumptionBlocked, FeatureQualityGate
from quant_research.features.quality import (
    FactorQualityMetric,
    FactorQualityReport,
    QualitySeverity,
    QualityStatus,
)


def factor_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="ret_1",
        version="1.0.0",
        namespace="price",
        description="One bar historical return.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=2,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
    )


def run_config(factor_run_id: str = "factor-run-1") -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id=factor_run_id,
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=("ret_1",),
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def factor_frame(*, include_as_of: bool = True) -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index, value in enumerate([None, 0.01]):
        row = {
            "dataset_id": "fixture-daily",
            "symbol": "000001.SZ",
            "freq": "1d",
            "ret_1": value,
        }
        if include_as_of:
            row["as_of"] = start + timedelta(days=index)
        rows.append(row)
    return pl.DataFrame(rows).lazy()


def commit_feature_run(
    store: LocalDuckDBFeatureStore,
    *,
    factor_run_id: str = "factor-run-1",
    include_as_of: bool = True,
):
    return store.commit_run(
        FeatureCommitRequest(
            config=run_config(factor_run_id),
            factor_frame=factor_frame(include_as_of=include_as_of),
            resolved_factors=(RegisteredFactor(factor_spec(), compute=None),),
            input_row_count=2,
        )
    )


def quality_report(
    *,
    status: QualityStatus,
    severity: QualitySeverity,
    factor_run_id: str = "factor-run-1",
) -> FactorQualityReport:
    return FactorQualityReport(
        factor_run_id=factor_run_id,
        feature_set_id="basic_price_v1",
        status=status,
        metrics=(
            FactorQualityMetric(
                factor_run_id=factor_run_id,
                feature_set_id="basic_price_v1",
                factor_id="ret_1",
                output_field="ret_1",
                metric_name="null_ratio",
                metric_value=0.0,
                metric_json={},
                severity=severity,
                created_at="2026-07-08T00:00:00+00:00",
            ),
        ),
    )


def test_gate_allows_passed_snapshot_consumption(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)
    store.commit_quality_report(
        quality_report(status=QualityStatus.PASSED, severity=QualitySeverity.INFO)
    )

    snapshots = FeatureQualityGate(store).read_consumable_snapshot(commit.snapshot_ref)

    assert [snapshot.features for snapshot in snapshots] == [{"ret_1": None}, {"ret_1": 0.01}]


def test_feature_store_can_still_audit_read_failed_quality_snapshot(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)
    store.commit_quality_report(
        quality_report(status=QualityStatus.FAILED, severity=QualitySeverity.ERROR)
    )

    snapshots = store.read_snapshot(commit.snapshot_ref)

    assert len(snapshots) == 2


@pytest.mark.parametrize(
    ("status", "severity"),
    [
        (QualityStatus.FAILED, QualitySeverity.ERROR),
        (QualityStatus.WARNING, QualitySeverity.WARNING),
    ],
)
def test_gate_blocks_non_passed_quality_status(tmp_path, status, severity):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)
    store.commit_quality_report(quality_report(status=status, severity=severity))

    with pytest.raises(FeatureConsumptionBlocked) as exc:
        FeatureQualityGate(store).read_consumable_snapshot(commit.snapshot_ref)

    assert exc.value.code == "QUALITY_NOT_PASSED"


def test_gate_blocks_not_run_quality_status(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store)

    with pytest.raises(FeatureConsumptionBlocked) as exc:
        FeatureQualityGate(store).assert_snapshot_consumable(commit.snapshot_ref)

    assert exc.value.code == "QUALITY_NOT_PASSED"


def test_gate_blocks_failed_feature_run_manifest(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = commit_feature_run(store, include_as_of=False)
    snapshot_ref = DataRef(
        "feature_snapshot",
        {
            "feature_set_id": "basic_price_v1",
            "factor_run_id": "factor-run-1",
            "dataset_id": "fixture-daily",
            "freq": "1d",
        },
    )

    assert commit.status == FeatureRunStatus.FAILED
    with pytest.raises(FeatureConsumptionBlocked) as exc:
        FeatureQualityGate(store).assert_snapshot_consumable(snapshot_ref)

    assert exc.value.code == "RUN_NOT_COMMITTED"


def test_gate_blocks_snapshot_ref_without_factor_run_id(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")

    with pytest.raises(FeatureConsumptionBlocked) as exc:
        FeatureQualityGate(store).assert_snapshot_consumable(
            "duckdb://feature_snapshot?feature_set_id=basic_price_v1"
        )

    assert exc.value.code == "MISSING_FACTOR_RUN_ID"


def test_gate_blocks_snapshot_ref_with_missing_manifest(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")

    with pytest.raises(FeatureConsumptionBlocked) as exc:
        FeatureQualityGate(store).assert_snapshot_consumable(
            "duckdb://feature_snapshot?factor_run_id=missing-run"
        )

    assert exc.value.code == "MISSING_MANIFEST"
