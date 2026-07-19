from __future__ import annotations

import json
from pathlib import Path

import duckdb

from quant_research.contracts.refs import DataRef
from quant_research.evaluation.contracts import (
    EvaluationMetricKind,
    EvaluationMetricStatus,
    EvaluationRunStatus,
    FactorEvaluationCommitResult,
    FactorEvaluationError,
    FactorEvaluationManifest,
    FactorEvaluationMetric,
    LongShortDirection,
)


_MANIFEST_COLUMNS = (
    "evaluation_run_id",
    "feature_snapshot_ref",
    "label_ref",
    "factor_run_id",
    "label_run_id",
    "dataset_id",
    "freq",
    "factor_fields_json",
    "label_field",
    "quantile_count",
    "minimum_cross_section_size",
    "long_short_direction",
    "rank_tie_method",
    "quantile_tie_breaker",
    "row_count_aligned",
    "evaluated_cross_section_count",
    "skipped_cross_section_count",
    "metric_count",
    "status",
    "created_at",
    "code_version",
    "config_hash",
    "content_hash",
    "metric_ref",
)

_METRIC_COLUMNS = (
    "evaluation_run_id",
    "factor_field",
    "label_field",
    "as_of",
    "metric_kind",
    "metric_status",
    "metric_value",
    "sample_count",
    "quantile",
)


class LocalDuckDBEvaluationStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_run(
        self,
        manifest: FactorEvaluationManifest,
        metrics: tuple[FactorEvaluationMetric, ...],
    ) -> FactorEvaluationCommitResult:
        existing = self.get_manifest(manifest.evaluation_run_id)
        if existing is not None:
            if (
                existing.config_hash == manifest.config_hash
                and existing.content_hash == manifest.content_hash
            ):
                return FactorEvaluationCommitResult(existing, reused_existing=True)
            raise FactorEvaluationError(
                "EVALUATION_RUN_CONFLICT",
                "evaluation_run_id already exists with different configuration or content",
            )

        if len(metrics) != manifest.metric_count:
            raise FactorEvaluationError(
                "METRIC_COUNT_MISMATCH",
                "manifest metric_count does not match metric rows",
            )
        if any(metric.evaluation_run_id != manifest.evaluation_run_id for metric in metrics):
            raise FactorEvaluationError(
                "METRIC_RUN_ID_MISMATCH",
                "all metric rows must belong to the manifest evaluation_run_id",
            )

        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._insert_manifest(conn, manifest)
                self._insert_metrics(conn, metrics)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return FactorEvaluationCommitResult(manifest)

    def get_manifest(self, evaluation_run_id: str) -> FactorEvaluationManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {", ".join(_MANIFEST_COLUMNS)}
                FROM factor_evaluation_manifest
                WHERE evaluation_run_id = ?
                """,
                [evaluation_run_id],
            ).fetchone()
        return self._row_to_manifest(row) if row else None

    def read_metrics(self, ref: DataRef | str) -> list[FactorEvaluationMetric]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        if data_ref.table != "factor_evaluation_metric":
            raise ValueError(f"unsupported evaluation metric ref: {data_ref.table}")
        where_sql, params = self._where_clause(data_ref.filters)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_METRIC_COLUMNS)}
                FROM factor_evaluation_metric
                {where_sql}
                ORDER BY factor_field, as_of, metric_kind, quantile
                """,
                params,
            ).fetchall()
        return [self._row_to_metric(row) for row in rows]

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS factor_evaluation_manifest (
                    evaluation_run_id VARCHAR PRIMARY KEY,
                    feature_snapshot_ref VARCHAR NOT NULL,
                    label_ref VARCHAR NOT NULL,
                    factor_run_id VARCHAR NOT NULL,
                    label_run_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    factor_fields_json VARCHAR NOT NULL,
                    label_field VARCHAR NOT NULL,
                    quantile_count BIGINT NOT NULL,
                    minimum_cross_section_size BIGINT NOT NULL,
                    long_short_direction VARCHAR NOT NULL,
                    rank_tie_method VARCHAR NOT NULL,
                    quantile_tie_breaker VARCHAR NOT NULL,
                    row_count_aligned BIGINT NOT NULL,
                    evaluated_cross_section_count BIGINT NOT NULL,
                    skipped_cross_section_count BIGINT NOT NULL,
                    metric_count BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    code_version VARCHAR NOT NULL,
                    config_hash VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    metric_ref VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS factor_evaluation_metric (
                    evaluation_run_id VARCHAR NOT NULL,
                    factor_field VARCHAR NOT NULL,
                    label_field VARCHAR NOT NULL,
                    as_of VARCHAR NOT NULL,
                    metric_kind VARCHAR NOT NULL,
                    metric_status VARCHAR NOT NULL,
                    metric_value DOUBLE,
                    sample_count BIGINT NOT NULL,
                    quantile BIGINT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_factor_evaluation_metric_lookup
                ON factor_evaluation_metric (evaluation_run_id, factor_field, as_of)
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _insert_manifest(self, conn, manifest: FactorEvaluationManifest) -> None:
        placeholders = ", ".join(["?"] * len(_MANIFEST_COLUMNS))
        conn.execute(
            f"""
            INSERT INTO factor_evaluation_manifest ({", ".join(_MANIFEST_COLUMNS)})
            VALUES ({placeholders})
            """,
            self._manifest_to_row(manifest),
        )

    def _insert_metrics(
        self,
        conn,
        metrics: tuple[FactorEvaluationMetric, ...],
    ) -> None:
        if not metrics:
            return
        placeholders = ", ".join(["?"] * len(_METRIC_COLUMNS))
        conn.executemany(
            f"""
            INSERT INTO factor_evaluation_metric ({", ".join(_METRIC_COLUMNS)})
            VALUES ({placeholders})
            """,
            [self._metric_to_row(metric) for metric in metrics],
        )

    def _where_clause(self, filters: dict[str, str]) -> tuple[str, list[str]]:
        allowed = {
            "evaluation_run_id",
            "factor_field",
            "label_field",
            "as_of",
            "metric_kind",
            "metric_status",
            "quantile",
        }
        unsupported = set(filters) - allowed
        if unsupported:
            raise ValueError(f"unsupported evaluation metric filters: {sorted(unsupported)}")
        if not filters:
            return "", []
        return "WHERE " + " AND ".join(f"{field} = ?" for field in filters), list(filters.values())

    def _manifest_to_row(self, manifest: FactorEvaluationManifest) -> tuple[object, ...]:
        return (
            manifest.evaluation_run_id,
            manifest.feature_snapshot_ref,
            manifest.label_ref,
            manifest.factor_run_id,
            manifest.label_run_id,
            manifest.dataset_id,
            manifest.freq,
            json.dumps(list(manifest.factor_fields), sort_keys=True),
            manifest.label_field,
            manifest.quantile_count,
            manifest.minimum_cross_section_size,
            manifest.long_short_direction.value,
            manifest.rank_tie_method,
            manifest.quantile_tie_breaker,
            manifest.row_count_aligned,
            manifest.evaluated_cross_section_count,
            manifest.skipped_cross_section_count,
            manifest.metric_count,
            manifest.status.value,
            manifest.created_at,
            manifest.code_version,
            manifest.config_hash,
            manifest.content_hash,
            manifest.metric_ref,
        )

    def _row_to_manifest(self, row) -> FactorEvaluationManifest:
        return FactorEvaluationManifest(
            evaluation_run_id=row[0],
            feature_snapshot_ref=row[1],
            label_ref=row[2],
            factor_run_id=row[3],
            label_run_id=row[4],
            dataset_id=row[5],
            freq=row[6],
            factor_fields=tuple(json.loads(row[7])),
            label_field=row[8],
            quantile_count=row[9],
            minimum_cross_section_size=row[10],
            long_short_direction=LongShortDirection(row[11]),
            rank_tie_method=row[12],
            quantile_tie_breaker=row[13],
            row_count_aligned=row[14],
            evaluated_cross_section_count=row[15],
            skipped_cross_section_count=row[16],
            metric_count=row[17],
            status=EvaluationRunStatus(row[18]),
            created_at=row[19],
            code_version=row[20],
            config_hash=row[21],
            content_hash=row[22],
            metric_ref=row[23],
        )

    def _metric_to_row(self, metric: FactorEvaluationMetric) -> tuple[object, ...]:
        return (
            metric.evaluation_run_id,
            metric.factor_field,
            metric.label_field,
            metric.as_of,
            metric.metric_kind.value,
            metric.metric_status.value,
            metric.metric_value,
            metric.sample_count,
            metric.quantile,
        )

    def _row_to_metric(self, row) -> FactorEvaluationMetric:
        return FactorEvaluationMetric(
            evaluation_run_id=row[0],
            factor_field=row[1],
            label_field=row[2],
            as_of=row[3],
            metric_kind=EvaluationMetricKind(row[4]),
            metric_status=EvaluationMetricStatus(row[5]),
            metric_value=row[6],
            sample_count=row[7],
            quantile=row[8],
        )
