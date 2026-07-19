from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from quant_research.datasets.contracts import (
    MaterializedDatasetStatus,
    MaterializedTrainingDatasetManifest,
    TrainingDatasetError,
)
from quant_research.training.contracts import (
    OOSAssignment,
    WalkForwardError,
    WalkForwardFold,
    WalkForwardSplitPlan,
    WalkForwardSplitResult,
    WalkForwardWindowMode,
)


@dataclass(frozen=True)
class WalkForwardSplitter:
    temporal_field: str = "as_of"

    def split(
        self,
        dataset: MaterializedTrainingDatasetManifest,
        plan: WalkForwardSplitPlan,
    ) -> WalkForwardSplitResult:
        path = self._validate_dataset(dataset)
        periods = self._read_periods(path)
        gap_periods = plan.purge_periods + plan.embargo_periods
        test_start_index = plan.train_periods + gap_periods
        folds: list[WalkForwardFold] = []
        assignments: list[OOSAssignment] = []

        while test_start_index + plan.test_periods <= len(periods):
            train_end_exclusive = test_start_index - gap_periods
            train_start_index = (
                0
                if plan.window_mode is WalkForwardWindowMode.EXPANDING
                else train_end_exclusive - plan.train_periods
            )
            if train_start_index < 0 or train_end_exclusive <= train_start_index:
                raise WalkForwardError(
                    "INVALID_FOLD_BOUNDARY",
                    "split plan produced an empty or negative training window",
                )

            test_end_exclusive = test_start_index + plan.test_periods
            fold_id = f"fold-{len(folds):03d}"
            purge_start_index = train_end_exclusive
            purge_end_exclusive = purge_start_index + plan.purge_periods
            embargo_start_index = purge_end_exclusive
            embargo_end_exclusive = embargo_start_index + plan.embargo_periods
            test_period_values = periods[test_start_index:test_end_exclusive]

            folds.append(
                WalkForwardFold(
                    fold_id=fold_id,
                    train_start=periods[train_start_index],
                    train_end=periods[train_end_exclusive - 1],
                    test_start=test_period_values[0],
                    test_end=test_period_values[-1],
                    train_period_count=train_end_exclusive - train_start_index,
                    test_period_count=len(test_period_values),
                    purge_start=_range_start(periods, purge_start_index, plan.purge_periods),
                    purge_end=_range_end(periods, purge_end_exclusive, plan.purge_periods),
                    embargo_start=_range_start(
                        periods,
                        embargo_start_index,
                        plan.embargo_periods,
                    ),
                    embargo_end=_range_end(
                        periods,
                        embargo_end_exclusive,
                        plan.embargo_periods,
                    ),
                )
            )
            assignments.extend(
                OOSAssignment(fold_id=fold_id, as_of=as_of) for as_of in test_period_values
            )
            test_start_index += plan.resolved_step_periods

        if not folds:
            raise WalkForwardError(
                "INSUFFICIENT_PERIODS",
                "materialized dataset does not contain one complete walk-forward fold",
            )
        _assert_unique_oos(assignments)
        return WalkForwardSplitResult(
            materialized_dataset_id=dataset.materialized_dataset_id,
            plan=plan,
            folds=tuple(folds),
            oos_assignments=tuple(assignments),
        )

    def _validate_dataset(self, dataset: MaterializedTrainingDatasetManifest) -> Path:
        if dataset.status is not MaterializedDatasetStatus.COMMITTED:
            raise WalkForwardError(
                "UNCOMMITTED_DATASET",
                "walk-forward splitting requires a committed materialized dataset",
            )
        if dataset.artifact_format != "parquet":
            raise WalkForwardError(
                "UNSUPPORTED_DATASET_FORMAT",
                f"walk-forward splitting requires parquet, got {dataset.artifact_format}",
            )
        if self.temporal_field not in dataset.schema_fields:
            raise WalkForwardError(
                "MISSING_TEMPORAL_FIELD",
                f"materialized dataset schema has no {self.temporal_field} field",
            )
        try:
            path = dataset.artifact_path
        except TrainingDatasetError as exc:
            raise WalkForwardError("INVALID_ARTIFACT_REF", exc.message) from exc
        if not path.is_file():
            raise WalkForwardError(
                "MATERIALIZED_ARTIFACT_MISSING",
                f"materialized dataset artifact is missing: {path}",
            )
        if _file_hash(path) != dataset.artifact_hash:
            raise WalkForwardError(
                "MATERIALIZED_ARTIFACT_HASH_MISMATCH",
                "materialized dataset artifact bytes do not match its manifest",
            )
        return path

    def _read_periods(self, path: Path) -> list[str]:
        try:
            temporal = (
                pl.scan_parquet(path)
                .select(pl.col(self.temporal_field))
                .unique()
                .sort(self.temporal_field)
                .collect()
            )
        except (pl.exceptions.PolarsError, OSError) as exc:
            raise WalkForwardError(
                "UNREADABLE_MATERIALIZED_DATASET",
                f"could not read materialized dataset: {exc}",
            ) from exc
        if temporal.height == 0:
            raise WalkForwardError(
                "EMPTY_MATERIALIZED_DATASET",
                "materialized dataset contains no observed periods",
            )
        values = temporal[self.temporal_field].to_list()
        if any(value is None for value in values):
            raise WalkForwardError(
                "NULL_TEMPORAL_FIELD",
                f"materialized dataset contains null {self.temporal_field} values",
            )
        return [_canonical_period(value) for value in values]


def _canonical_period(value: object) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _range_start(periods: list[str], start: int, length: int) -> str | None:
    return periods[start] if length else None


def _range_end(periods: list[str], end_exclusive: int, length: int) -> str | None:
    return periods[end_exclusive - 1] if length else None


def _assert_unique_oos(assignments: list[OOSAssignment]) -> None:
    as_of_values = [assignment.as_of for assignment in assignments]
    if len(as_of_values) != len(set(as_of_values)):
        raise WalkForwardError(
            "OVERLAPPING_OOS_WINDOWS",
            "one or more timestamps are assigned to multiple OOS folds",
        )


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"
