from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from quant_research.contracts.refs import DataRef
from quant_research.labels.contracts import (
    LabelCommitRequest,
    LabelRunManifest,
    LabelSourceKind,
    LabelStoreError,
    LabelValue,
)
from quant_research.labels.quality import LabelQualityMetric, LabelQualityReport


_LABEL_COLUMNS = (
    "label_run_id",
    "label_set_id",
    "dataset_id",
    "symbol",
    "freq",
    "as_of",
    "label_id",
    "label_version",
    "value_float",
    "value_string",
    "value_kind",
    "forward_bars",
    "source_factor_run_id",
    "created_at",
    "source_kind",
    "source_ref",
)

_MANIFEST_COLUMNS = (
    "label_run_id",
    "label_set_id",
    "source_factor_run_id",
    "row_count_label",
    "status",
    "created_at",
    "quality_status",
    "quality_summary_json",
    "source_kind",
    "source_ref",
    "dataset_id",
    "freq",
    "forward_bars",
    "source_as_of_start",
    "source_as_of_end",
    "market_data_ref",
    "market_dataset_version",
    "market_data_definition_hash",
    "market_data_snapshot_set_hash",
    "universe_ref",
    "universe_id",
    "universe_version",
    "universe_definition_hash",
    "universe_snapshot_set_hash",
)

_QUALITY_METRIC_COLUMNS = (
    "label_run_id",
    "label_set_id",
    "label_id",
    "metric_name",
    "metric_value",
    "metric_json",
    "severity",
    "created_at",
)


class LocalDuckDBLabelStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_labels(self, request: LabelCommitRequest) -> DataRef:
        self.validate_labels(request.labels)
        manifest = LabelRunManifest(
            label_run_id=request.label_run_id,
            label_set_id=request.label_set_id,
            source_factor_run_id=request.source_factor_run_id,
            row_count_label=len(request.labels),
            status="COMMITTED",
            created_at=datetime.now(UTC).isoformat(),
            source_kind=request.source_kind,
            source_ref=request.source_ref or request.source_factor_run_id,
            dataset_id=request.dataset_id,
            freq=request.freq,
            forward_bars=request.forward_bars,
            source_as_of_start=request.source_as_of_start,
            source_as_of_end=request.source_as_of_end,
            market_data_ref=request.market_data_ref,
            market_dataset_version=request.market_dataset_version,
            market_data_definition_hash=request.market_data_definition_hash,
            market_data_snapshot_set_hash=request.market_data_snapshot_set_hash,
            universe_ref=request.universe_ref,
            universe_id=request.universe_id,
            universe_version=request.universe_version,
            universe_definition_hash=request.universe_definition_hash,
            universe_snapshot_set_hash=request.universe_snapshot_set_hash,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM label_table WHERE label_run_id = ?", [request.label_run_id])
                conn.execute(
                    "DELETE FROM label_run_manifest WHERE label_run_id = ?",
                    [request.label_run_id],
                )
                self._insert_labels(conn, list(request.labels))
                placeholders = ", ".join(["?"] * len(_MANIFEST_COLUMNS))
                conn.execute(
                    f"""
                    INSERT INTO label_run_manifest ({", ".join(_MANIFEST_COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    self._manifest_to_row(manifest),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return DataRef("label_table", {"label_run_id": request.label_run_id})

    def commit_quality_report(self, report: LabelQualityReport) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "DELETE FROM label_quality_metric WHERE label_run_id = ?",
                    [report.label_run_id],
                )
                self._insert_quality_metrics(conn, list(report.metrics))
                conn.execute(
                    """
                    UPDATE label_run_manifest
                    SET quality_status = ?, quality_summary_json = ?
                    WHERE label_run_id = ?
                    """,
                    [
                        report.status.value,
                        json.dumps(report.summary, sort_keys=True),
                        report.label_run_id,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def list_quality_metrics(self, label_run_id: str) -> list[LabelQualityMetric]:
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_QUALITY_METRIC_COLUMNS)}
                FROM label_quality_metric
                WHERE label_run_id = ?
                ORDER BY label_id, metric_name
                """,
                [label_run_id],
            ).fetchall()
        return [self._row_to_quality_metric(row) for row in rows]

    def validate_labels(self, labels: tuple[LabelValue, ...]) -> None:
        seen: set[tuple[str, str, str, str, str, str, str, str]] = set()
        for label in labels:
            key = (
                label.label_set_id,
                label.dataset_id,
                label.symbol,
                label.freq,
                label.as_of,
                label.label_id,
                label.label_version,
                label.source_factor_run_id,
            )
            if key in seen:
                raise LabelStoreError("DUPLICATE_LABEL_KEY", "duplicate label key")
            seen.add(key)

    def read_labels(self, ref: DataRef | str) -> list[LabelValue]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        if data_ref.table != "label_table":
            raise ValueError(f"unsupported label table ref: {data_ref.table}")
        where_sql, params = self._where_clause(data_ref.filters)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_LABEL_COLUMNS)}
                FROM label_table
                {where_sql}
                ORDER BY symbol, as_of, label_id
                """,
                params,
            ).fetchall()
        return [self._row_to_label(row) for row in rows]

    def get_manifest(self, label_run_id: str) -> LabelRunManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {", ".join(_MANIFEST_COLUMNS)}
                FROM label_run_manifest
                WHERE label_run_id = ?
                """,
                [label_run_id],
            ).fetchone()
        if row is None:
            return None
        return LabelRunManifest(
            label_run_id=row[0],
            label_set_id=row[1],
            source_factor_run_id=row[2],
            row_count_label=row[3],
            status=row[4],
            created_at=row[5],
            quality_status=row[6],
            quality_summary=json.loads(row[7]),
            source_kind=LabelSourceKind(row[8] or LabelSourceKind.LEGACY),
            source_ref=row[9] or row[2],
            dataset_id=row[10],
            freq=row[11],
            forward_bars=row[12],
            source_as_of_start=row[13],
            source_as_of_end=row[14],
            market_data_ref=row[15],
            market_dataset_version=row[16],
            market_data_definition_hash=row[17],
            market_data_snapshot_set_hash=row[18],
            universe_ref=row[19],
            universe_id=row[20],
            universe_version=row[21],
            universe_definition_hash=row[22],
            universe_snapshot_set_hash=row[23],
        )

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS label_table (
                    label_run_id VARCHAR NOT NULL,
                    label_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    as_of VARCHAR NOT NULL,
                    label_id VARCHAR NOT NULL,
                    label_version VARCHAR NOT NULL,
                    value_float DOUBLE,
                    value_string VARCHAR,
                    value_kind VARCHAR NOT NULL,
                    forward_bars BIGINT NOT NULL,
                    source_factor_run_id VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    source_kind VARCHAR,
                    source_ref VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS label_run_manifest (
                    label_run_id VARCHAR PRIMARY KEY,
                    label_set_id VARCHAR NOT NULL,
                    source_factor_run_id VARCHAR NOT NULL,
                    row_count_label BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    quality_status VARCHAR NOT NULL,
                    quality_summary_json VARCHAR NOT NULL
                )
                """
            )
            conn.execute("ALTER TABLE label_table ADD COLUMN IF NOT EXISTS source_kind VARCHAR")
            conn.execute("ALTER TABLE label_table ADD COLUMN IF NOT EXISTS source_ref VARCHAR")
            manifest_columns = {
                "source_kind": "VARCHAR",
                "source_ref": "VARCHAR",
                "dataset_id": "VARCHAR",
                "freq": "VARCHAR",
                "forward_bars": "BIGINT",
                "source_as_of_start": "VARCHAR",
                "source_as_of_end": "VARCHAR",
                "market_data_ref": "VARCHAR",
                "market_dataset_version": "VARCHAR",
                "market_data_definition_hash": "VARCHAR",
                "market_data_snapshot_set_hash": "VARCHAR",
                "universe_ref": "VARCHAR",
                "universe_id": "VARCHAR",
                "universe_version": "VARCHAR",
                "universe_definition_hash": "VARCHAR",
                "universe_snapshot_set_hash": "VARCHAR",
            }
            for column, sql_type in manifest_columns.items():
                conn.execute(
                    f"ALTER TABLE label_run_manifest ADD COLUMN IF NOT EXISTS {column} {sql_type}"
                )
            conn.execute(
                """
                UPDATE label_table
                SET source_kind = COALESCE(source_kind, 'LEGACY'),
                    source_ref = COALESCE(source_ref, source_factor_run_id)
                """
            )
            conn.execute(
                """
                UPDATE label_run_manifest
                SET source_kind = COALESCE(source_kind, 'LEGACY'),
                    source_ref = COALESCE(source_ref, source_factor_run_id)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS label_quality_metric (
                    label_run_id VARCHAR NOT NULL,
                    label_set_id VARCHAR NOT NULL,
                    label_id VARCHAR NOT NULL,
                    metric_name VARCHAR NOT NULL,
                    metric_value DOUBLE NOT NULL,
                    metric_json VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _insert_labels(self, conn, labels: list[LabelValue]) -> None:
        if not labels:
            return
        placeholders = ", ".join(["?"] * len(_LABEL_COLUMNS))
        conn.executemany(
            f"""
            INSERT INTO label_table ({", ".join(_LABEL_COLUMNS)})
            VALUES ({placeholders})
            """,
            [self._label_to_row(label) for label in labels],
        )

    def _insert_quality_metrics(
        self,
        conn,
        metrics: list[LabelQualityMetric],
    ) -> None:
        if not metrics:
            return
        placeholders = ", ".join(["?"] * len(_QUALITY_METRIC_COLUMNS))
        conn.executemany(
            f"""
            INSERT INTO label_quality_metric ({", ".join(_QUALITY_METRIC_COLUMNS)})
            VALUES ({placeholders})
            """,
            [self._quality_metric_to_row(metric) for metric in metrics],
        )

    def _where_clause(self, filters: dict[str, str]) -> tuple[str, list[str]]:
        allowed = {
            "label_run_id",
            "label_set_id",
            "dataset_id",
            "symbol",
            "freq",
            "as_of",
            "label_id",
        }
        unsupported = set(filters) - allowed
        if unsupported:
            raise ValueError(f"unsupported label_table filters: {sorted(unsupported)}")
        if not filters:
            return "", []
        return "WHERE " + " AND ".join(f"{field} = ?" for field in filters), list(
            filters.values()
        )

    def _label_to_row(self, label: LabelValue) -> tuple[object, ...]:
        return (
            label.label_run_id,
            label.label_set_id,
            label.dataset_id,
            label.symbol,
            label.freq,
            label.as_of,
            label.label_id,
            label.label_version,
            label.value_float,
            label.value_string,
            label.value_kind,
            label.forward_bars,
            label.source_factor_run_id,
            label.created_at,
            label.source_kind.value,
            label.source_ref or label.source_factor_run_id,
        )

    def _row_to_label(self, row) -> LabelValue:
        return LabelValue(
            label_run_id=row[0],
            label_set_id=row[1],
            dataset_id=row[2],
            symbol=row[3],
            freq=row[4],
            as_of=row[5],
            label_id=row[6],
            label_version=row[7],
            value_float=row[8],
            value_string=row[9],
            value_kind=row[10],
            forward_bars=row[11],
            source_factor_run_id=row[12],
            created_at=row[13],
            source_kind=LabelSourceKind(row[14] or LabelSourceKind.LEGACY),
            source_ref=row[15] or row[12],
        )

    def _manifest_to_row(self, manifest: LabelRunManifest) -> tuple[object, ...]:
        return (
            manifest.label_run_id,
            manifest.label_set_id,
            manifest.source_factor_run_id,
            manifest.row_count_label,
            manifest.status,
            manifest.created_at,
            manifest.quality_status,
            json.dumps(manifest.quality_summary, sort_keys=True),
            manifest.source_kind.value,
            manifest.source_ref,
            manifest.dataset_id,
            manifest.freq,
            manifest.forward_bars,
            manifest.source_as_of_start,
            manifest.source_as_of_end,
            manifest.market_data_ref,
            manifest.market_dataset_version,
            manifest.market_data_definition_hash,
            manifest.market_data_snapshot_set_hash,
            manifest.universe_ref,
            manifest.universe_id,
            manifest.universe_version,
            manifest.universe_definition_hash,
            manifest.universe_snapshot_set_hash,
        )

    def _quality_metric_to_row(self, metric: LabelQualityMetric) -> tuple[object, ...]:
        return (
            metric.label_run_id,
            metric.label_set_id,
            metric.label_id,
            metric.metric_name,
            metric.metric_value,
            json.dumps(metric.metric_json, sort_keys=True),
            metric.severity.value,
            metric.created_at,
        )

    def _row_to_quality_metric(self, row) -> LabelQualityMetric:
        from quant_research.features.quality import QualitySeverity

        return LabelQualityMetric(
            label_run_id=row[0],
            label_set_id=row[1],
            label_id=row[2],
            metric_name=row[3],
            metric_value=row[4],
            metric_json=json.loads(row[5]),
            severity=QualitySeverity(row[6]),
            created_at=row[7],
        )
