from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import polars as pl
import pytest

from quant_research.datasets.contracts import (
    MaterializeTrainingDatasetRequest,
    TrainingDatasetManifest,
    TrainingDatasetStatus,
)
from quant_research.datasets.duckdb_store import LocalDuckDBTrainingDatasetStore
from quant_research.datasets.feature_matrix import TrainingDatasetBuildResult
from quant_research.datasets.materialization import TrainingDatasetMaterializer
from quant_research.training import (
    WalkForwardError,
    WalkForwardSplitPlan,
    WalkForwardSplitter,
    WalkForwardWindowMode,
)


def _materialized_dataset(tmp_path, *, period_count: int = 12):
    rows = []
    first = date(2026, 1, 1)
    for offset in range(period_count):
        as_of = (first + timedelta(days=offset)).isoformat()
        for symbol in ("000001.SZ", "000002.SZ"):
            rows.append(
                {
                    "feature_set_id": "features-v1",
                    "dataset_id": "fixture-daily",
                    "symbol": symbol,
                    "freq": "1d",
                    "as_of": as_of,
                    "warmup_complete": True,
                    "feature_ref": f"feature-{symbol}-{as_of}",
                    "factor": float(offset),
                    "target": float(offset + 1),
                }
            )
    build = TrainingDatasetBuildResult(
        matrix=pl.DataFrame(rows).lazy(),
        manifest=TrainingDatasetManifest(
            training_dataset_id="training-v1",
            feature_ref="feature-ref",
            label_ref="label-ref",
            factor_run_id="factor-run",
            label_run_id="label-run",
            feature_set_id="features-v1",
            label_set_id="labels-v1",
            dataset_id="fixture-daily",
            freq="1d",
            feature_fields=("factor",),
            label_fields=("target",),
            row_count_feature=len(rows),
            row_count_label=len(rows),
            row_count_joined=len(rows),
            row_count_feature_only=0,
            row_count_label_only=0,
            content_hash="sha256:source",
            status=TrainingDatasetStatus.COMMITTED,
            created_at="2026-07-19T00:00:00+00:00",
        ),
    )
    store = LocalDuckDBTrainingDatasetStore(tmp_path / "research.duckdb")
    return (
        TrainingDatasetMaterializer(store)
        .materialize(
            build,
            MaterializeTrainingDatasetRequest("materialized-v1", tmp_path / "artifacts"),
        )
        .manifest
    )


def test_rolling_split_has_fixed_windows_purge_embargo_and_unique_oos(tmp_path):
    dataset = _materialized_dataset(tmp_path)
    plan = WalkForwardSplitPlan(
        train_periods=4,
        test_periods=2,
        step_periods=2,
        purge_periods=1,
        embargo_periods=1,
    )

    result = WalkForwardSplitter().split(dataset, plan)

    assert len(result.folds) == 3
    assert [fold.train_period_count for fold in result.folds] == [4, 4, 4]
    first = result.folds[0]
    assert first.train_start == "2026-01-01"
    assert first.train_end == "2026-01-04"
    assert first.purge_start == first.purge_end == "2026-01-05"
    assert first.embargo_start == first.embargo_end == "2026-01-06"
    assert first.test_start == "2026-01-07"
    assert first.test_end == "2026-01-08"
    oos_dates = [assignment.as_of for assignment in result.oos_assignments]
    assert len(oos_dates) == 6
    assert len(oos_dates) == len(set(oos_dates))
    assert oos_dates == [
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-10",
        "2026-01-11",
        "2026-01-12",
    ]


def test_expanding_split_grows_training_and_omits_incomplete_tail(tmp_path):
    dataset = _materialized_dataset(tmp_path, period_count=11)
    plan = WalkForwardSplitPlan(
        train_periods=4,
        test_periods=2,
        step_periods=2,
        window_mode=WalkForwardWindowMode.EXPANDING,
    )

    first = WalkForwardSplitter().split(dataset, plan)
    second = WalkForwardSplitter().split(dataset, plan)

    assert first == second
    assert [fold.train_period_count for fold in first.folds] == [4, 6, 8]
    assert {fold.train_start for fold in first.folds} == {"2026-01-01"}
    assert first.folds[-1].test_end == "2026-01-10"
    assert "2026-01-11" not in {item.as_of for item in first.oos_assignments}


@pytest.mark.parametrize(
    "kwargs, code",
    [
        ({"train_periods": 0, "test_periods": 1}, "INVALID_SPLIT_PLAN"),
        (
            {"train_periods": 2, "test_periods": 2, "step_periods": 1},
            "OVERLAPPING_OOS_WINDOWS",
        ),
        (
            {"train_periods": 2, "test_periods": 1, "purge_periods": -1},
            "INVALID_SPLIT_PLAN",
        ),
    ],
)
def test_invalid_split_plans_are_rejected(kwargs, code):
    with pytest.raises(WalkForwardError) as exc_info:
        WalkForwardSplitPlan(**kwargs)

    assert exc_info.value.code == code


def test_splitter_rejects_missing_tampered_and_incomplete_dataset_contracts(tmp_path):
    dataset = _materialized_dataset(tmp_path)
    splitter = WalkForwardSplitter()
    plan = WalkForwardSplitPlan(train_periods=4, test_periods=2)

    with pytest.raises(WalkForwardError) as exc_info:
        splitter.split(replace(dataset, schema_fields=("symbol", "factor")), plan)
    assert exc_info.value.code == "MISSING_TEMPORAL_FIELD"

    dataset.artifact_path.write_bytes(b"tampered")
    with pytest.raises(WalkForwardError) as exc_info:
        splitter.split(dataset, plan)
    assert exc_info.value.code == "MATERIALIZED_ARTIFACT_HASH_MISMATCH"

    dataset.artifact_path.unlink()
    with pytest.raises(WalkForwardError) as exc_info:
        splitter.split(dataset, plan)
    assert exc_info.value.code == "MATERIALIZED_ARTIFACT_MISSING"


def test_splitter_rejects_dataset_without_complete_fold(tmp_path):
    dataset = _materialized_dataset(tmp_path, period_count=5)

    with pytest.raises(WalkForwardError) as exc_info:
        WalkForwardSplitter().split(
            dataset,
            WalkForwardSplitPlan(
                train_periods=4,
                test_periods=1,
                purge_periods=1,
            ),
        )

    assert exc_info.value.code == "INSUFFICIENT_PERIODS"
