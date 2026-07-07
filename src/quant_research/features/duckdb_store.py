from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from quant_research.contracts.refs import DataRef
from quant_research.features.contracts import (
    FeatureCommitRequest,
    FeatureCommitResult,
    FeatureRunManifest,
    FeatureRunStatus,
    FeatureSnapshot,
    FeatureStoreError,
    FeatureValue,
)
from quant_research.features.transform import build_feature_snapshots, wide_to_feature_values


_FEATURE_VALUE_COLUMNS = (
    "factor_run_id",
    "feature_set_id",
    "dataset_id",
    "symbol",
    "freq",
    "as_of",
    "factor_id",
    "factor_version",
    "output_field",
    "value_float",
    "value_string",
    "value_kind",
    "warmup_complete",
    "quality_flags_json",
    "input_data_ref",
    "created_at",
)

_SNAPSHOT_COLUMNS = (
    "snapshot_id",
    "feature_set_id",
    "dataset_id",
    "symbol",
    "freq",
    "as_of",
    "features_json",
    "factor_run_ids_json",
    "input_data_refs_json",
    "warmup_complete",
    "quality_flags_json",
    "feature_ref",
    "created_at",
)

_MANIFEST_COLUMNS = (
    "factor_run_id",
    "feature_set_id",
    "dataset_id",
    "freq",
    "input_data_refs_json",
    "factor_versions_json",
    "factor_output_fields_json",
    "engine",
    "execution_mode",
    "status",
    "started_at",
    "finished_at",
    "row_count_input",
    "row_count_feature",
    "row_count_snapshot",
    "error_code",
    "error_message",
)


class LocalDuckDBFeatureStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_run(self, request: FeatureCommitRequest) -> FeatureCommitResult:
        existing = self.get_manifest(request.config.factor_run_id)
        feature_table_ref = self._feature_table_ref(request.config.factor_run_id)
        manifest_ref = self._manifest_ref(request.config.factor_run_id)
        if existing and existing.status == FeatureRunStatus.COMMITTED:
            return FeatureCommitResult(
                factor_run_id=request.config.factor_run_id,
                status=FeatureRunStatus.FAILED,
                snapshot_ref=None,
                feature_table_ref=feature_table_ref,
                manifest_ref=manifest_ref,
                row_count_feature=0,
                row_count_snapshot=0,
                error_code="FEATURE_RUN_ALREADY_COMMITTED",
                error_message="feature run is already committed",
            )
        if existing and existing.status == FeatureRunStatus.FAILED and not request.allow_failed_overwrite:
            return self._failed_result(
                request,
                "FEATURE_RUN_ALREADY_FAILED",
                "feature run already failed; set allow_failed_overwrite to replace it",
            )

        try:
            values = wide_to_feature_values(request)
            snapshots = build_feature_snapshots(request.config, values)
            snapshot_ref = self._snapshot_ref(request)
            committed = self._manifest(
                request,
                status=FeatureRunStatus.COMMITTED,
                row_count_feature=len(values),
                row_count_snapshot=len(snapshots),
            )
            with self._connect() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    self._delete_run_rows(conn, request.config.factor_run_id)
                    self._replace_manifest(conn, committed)
                    self._insert_feature_values(conn, values)
                    self._insert_snapshots(conn, snapshots)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        except FeatureStoreError as exc:
            return self._failed_result(request, exc.code, exc.message)
        except Exception as exc:
            return self._failed_result(request, "FEATURE_STORE_WRITE_FAILED", str(exc))

        return FeatureCommitResult(
            factor_run_id=request.config.factor_run_id,
            status=FeatureRunStatus.COMMITTED,
            snapshot_ref=snapshot_ref,
            feature_table_ref=feature_table_ref,
            manifest_ref=manifest_ref,
            row_count_feature=len(values),
            row_count_snapshot=len(snapshots),
        )

    def get_manifest(self, factor_run_id: str) -> FeatureRunManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {", ".join(_MANIFEST_COLUMNS)}
                FROM factor_run_manifest
                WHERE factor_run_id = ?
                """,
                [factor_run_id],
            ).fetchone()
        return self._row_to_manifest(row) if row else None

    def read_feature_table(self, ref: DataRef | str) -> list[FeatureValue]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        if data_ref.table != "feature_table":
            raise ValueError(f"unsupported feature table ref: {data_ref.table}")
        where_sql, params = self._where_clause(data_ref.filters, {"factor_run_id"})
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_FEATURE_VALUE_COLUMNS)}
                FROM feature_table
                {where_sql}
                ORDER BY symbol, as_of, factor_id, output_field
                """,
                params,
            ).fetchall()
        return [self._row_to_feature_value(row) for row in rows]

    def read_snapshot(self, ref: DataRef | str) -> list[FeatureSnapshot]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        if data_ref.table != "feature_snapshot":
            raise ValueError(f"unsupported feature snapshot ref: {data_ref.table}")
        where_sql, params = self._snapshot_where_clause(data_ref.filters)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_SNAPSHOT_COLUMNS)}
                FROM feature_snapshot
                {where_sql}
                ORDER BY symbol, as_of
                """,
                params,
            ).fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS factor_run_manifest (
                    factor_run_id VARCHAR PRIMARY KEY,
                    feature_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    input_data_refs_json VARCHAR NOT NULL,
                    factor_versions_json VARCHAR NOT NULL,
                    factor_output_fields_json VARCHAR NOT NULL,
                    engine VARCHAR NOT NULL,
                    execution_mode VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR,
                    row_count_input BIGINT,
                    row_count_feature BIGINT NOT NULL,
                    row_count_snapshot BIGINT NOT NULL,
                    error_code VARCHAR,
                    error_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_table (
                    factor_run_id VARCHAR NOT NULL,
                    feature_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    as_of VARCHAR NOT NULL,
                    factor_id VARCHAR NOT NULL,
                    factor_version VARCHAR NOT NULL,
                    output_field VARCHAR NOT NULL,
                    value_float DOUBLE,
                    value_string VARCHAR,
                    value_kind VARCHAR NOT NULL,
                    warmup_complete BOOLEAN NOT NULL,
                    quality_flags_json VARCHAR NOT NULL,
                    input_data_ref VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_snapshot (
                    snapshot_id VARCHAR PRIMARY KEY,
                    feature_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    as_of VARCHAR NOT NULL,
                    features_json VARCHAR NOT NULL,
                    factor_run_ids_json VARCHAR NOT NULL,
                    input_data_refs_json VARCHAR NOT NULL,
                    warmup_complete BOOLEAN NOT NULL,
                    quality_flags_json VARCHAR NOT NULL,
                    feature_ref VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _failed_result(
        self,
        request: FeatureCommitRequest,
        error_code: str,
        error_message: str,
    ) -> FeatureCommitResult:
        manifest = self._manifest(
            request,
            status=FeatureRunStatus.FAILED,
            row_count_feature=0,
            row_count_snapshot=0,
            error_code=error_code,
            error_message=error_message,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._delete_run_rows(conn, request.config.factor_run_id)
                self._replace_manifest(conn, manifest)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return FeatureCommitResult(
            factor_run_id=request.config.factor_run_id,
            status=FeatureRunStatus.FAILED,
            snapshot_ref=None,
            feature_table_ref=self._feature_table_ref(request.config.factor_run_id),
            manifest_ref=self._manifest_ref(request.config.factor_run_id),
            row_count_feature=0,
            row_count_snapshot=0,
            error_code=error_code,
            error_message=error_message,
        )

    def _manifest(
        self,
        request: FeatureCommitRequest,
        *,
        status: FeatureRunStatus,
        row_count_feature: int,
        row_count_snapshot: int,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> FeatureRunManifest:
        now = datetime.now(UTC).isoformat()
        return FeatureRunManifest(
            factor_run_id=request.config.factor_run_id,
            feature_set_id=request.config.feature_set_id,
            dataset_id=request.config.dataset_id,
            freq=request.config.freq.value,
            input_data_refs=(request.config.input_data_ref,),
            factor_versions={
                registered.spec.factor_id: registered.spec.version
                for registered in request.resolved_factors
            },
            factor_output_fields={
                registered.spec.factor_id: registered.spec.output_fields
                for registered in request.resolved_factors
            },
            engine=request.config.engine,
            execution_mode=request.config.execution_mode,
            status=status,
            started_at=now,
            finished_at=now,
            row_count_input=request.input_row_count,
            row_count_feature=row_count_feature,
            row_count_snapshot=row_count_snapshot,
            error_code=error_code,
            error_message=error_message,
        )

    def _replace_manifest(self, conn, manifest: FeatureRunManifest) -> None:
        conn.execute(
            "DELETE FROM factor_run_manifest WHERE factor_run_id = ?",
            [manifest.factor_run_id],
        )
        placeholders = ", ".join(["?"] * len(_MANIFEST_COLUMNS))
        conn.execute(
            f"""
            INSERT INTO factor_run_manifest ({", ".join(_MANIFEST_COLUMNS)})
            VALUES ({placeholders})
            """,
            self._manifest_to_row(manifest),
        )

    def _delete_run_rows(self, conn, factor_run_id: str) -> None:
        conn.execute("DELETE FROM feature_table WHERE factor_run_id = ?", [factor_run_id])
        conn.execute(
            "DELETE FROM feature_snapshot WHERE factor_run_ids_json = ?",
            [json.dumps([factor_run_id], sort_keys=True)],
        )

    def _insert_feature_values(self, conn, values: list[FeatureValue]) -> None:
        if not values:
            return
        placeholders = ", ".join(["?"] * len(_FEATURE_VALUE_COLUMNS))
        conn.executemany(
            f"""
            INSERT INTO feature_table ({", ".join(_FEATURE_VALUE_COLUMNS)})
            VALUES ({placeholders})
            """,
            [self._feature_value_to_row(value) for value in values],
        )

    def _insert_snapshots(self, conn, snapshots: list[FeatureSnapshot]) -> None:
        if not snapshots:
            return
        placeholders = ", ".join(["?"] * len(_SNAPSHOT_COLUMNS))
        conn.executemany(
            f"""
            INSERT INTO feature_snapshot ({", ".join(_SNAPSHOT_COLUMNS)})
            VALUES ({placeholders})
            """,
            [self._snapshot_to_row(snapshot) for snapshot in snapshots],
        )

    def _feature_table_ref(self, factor_run_id: str) -> DataRef:
        return DataRef("feature_table", {"factor_run_id": factor_run_id})

    def _manifest_ref(self, factor_run_id: str) -> DataRef:
        return DataRef("factor_run_manifest", {"factor_run_id": factor_run_id})

    def _snapshot_ref(self, request: FeatureCommitRequest) -> DataRef:
        return DataRef(
            "feature_snapshot",
            {
                "feature_set_id": request.config.feature_set_id,
                "factor_run_id": request.config.factor_run_id,
                "dataset_id": request.config.dataset_id,
                "freq": request.config.freq.value,
            },
        )

    def _where_clause(
        self,
        filters: dict[str, str],
        allowed: set[str],
    ) -> tuple[str, list[str]]:
        unsupported = set(filters) - allowed
        if unsupported:
            raise ValueError(f"unsupported filters: {sorted(unsupported)}")
        if not filters:
            return "", []
        return "WHERE " + " AND ".join(f"{field} = ?" for field in filters), list(
            filters.values()
        )

    def _snapshot_where_clause(self, filters: dict[str, str]) -> tuple[str, list[str]]:
        allowed = {"feature_set_id", "factor_run_id", "dataset_id", "symbol", "freq", "as_of"}
        unsupported = set(filters) - allowed
        if unsupported:
            raise ValueError(f"unsupported feature_snapshot filters: {sorted(unsupported)}")
        clauses: list[str] = []
        params: list[str] = []
        for field, value in filters.items():
            if field == "factor_run_id":
                clauses.append("factor_run_ids_json = ?")
                params.append(json.dumps([value], sort_keys=True))
            else:
                clauses.append(f"{field} = ?")
                params.append(value)
        return ("WHERE " + " AND ".join(clauses), params) if clauses else ("", [])

    def _feature_value_to_row(self, value: FeatureValue) -> tuple[object, ...]:
        return (
            value.factor_run_id,
            value.feature_set_id,
            value.dataset_id,
            value.symbol,
            value.freq,
            value.as_of,
            value.factor_id,
            value.factor_version,
            value.output_field,
            value.value_float,
            value.value_string,
            value.value_kind,
            value.warmup_complete,
            json.dumps(list(value.quality_flags), sort_keys=True),
            value.input_data_ref,
            value.created_at,
        )

    def _row_to_feature_value(self, row) -> FeatureValue:
        return FeatureValue(
            factor_run_id=row[0],
            feature_set_id=row[1],
            dataset_id=row[2],
            symbol=row[3],
            freq=row[4],
            as_of=row[5],
            factor_id=row[6],
            factor_version=row[7],
            output_field=row[8],
            value_float=row[9],
            value_string=row[10],
            value_kind=row[11],
            warmup_complete=row[12],
            quality_flags=tuple(json.loads(row[13])),
            input_data_ref=row[14],
            created_at=row[15],
        )

    def _snapshot_to_row(self, snapshot: FeatureSnapshot) -> tuple[object, ...]:
        return (
            snapshot.snapshot_id,
            snapshot.feature_set_id,
            snapshot.dataset_id,
            snapshot.symbol,
            snapshot.freq,
            snapshot.as_of,
            json.dumps(snapshot.features, sort_keys=True),
            json.dumps(list(snapshot.factor_run_ids), sort_keys=True),
            json.dumps(list(snapshot.input_data_refs), sort_keys=True),
            snapshot.warmup_complete,
            json.dumps(list(snapshot.quality_flags), sort_keys=True),
            snapshot.feature_ref,
            snapshot.created_at,
        )

    def _row_to_snapshot(self, row) -> FeatureSnapshot:
        return FeatureSnapshot(
            snapshot_id=row[0],
            feature_set_id=row[1],
            dataset_id=row[2],
            symbol=row[3],
            freq=row[4],
            as_of=row[5],
            features=json.loads(row[6]),
            factor_run_ids=tuple(json.loads(row[7])),
            input_data_refs=tuple(json.loads(row[8])),
            warmup_complete=row[9],
            quality_flags=tuple(json.loads(row[10])),
            feature_ref=row[11],
            created_at=row[12],
        )

    def _manifest_to_row(self, manifest: FeatureRunManifest) -> tuple[object, ...]:
        return (
            manifest.factor_run_id,
            manifest.feature_set_id,
            manifest.dataset_id,
            manifest.freq,
            json.dumps(list(manifest.input_data_refs), sort_keys=True),
            json.dumps(manifest.factor_versions, sort_keys=True),
            json.dumps(
                {key: list(value) for key, value in manifest.factor_output_fields.items()},
                sort_keys=True,
            ),
            manifest.engine,
            manifest.execution_mode,
            manifest.status.value,
            manifest.started_at,
            manifest.finished_at,
            manifest.row_count_input,
            manifest.row_count_feature,
            manifest.row_count_snapshot,
            manifest.error_code,
            manifest.error_message,
        )

    def _row_to_manifest(self, row) -> FeatureRunManifest:
        return FeatureRunManifest(
            factor_run_id=row[0],
            feature_set_id=row[1],
            dataset_id=row[2],
            freq=row[3],
            input_data_refs=tuple(json.loads(row[4])),
            factor_versions=json.loads(row[5]),
            factor_output_fields={
                key: tuple(value) for key, value in json.loads(row[6]).items()
            },
            engine=row[7],
            execution_mode=row[8],
            status=FeatureRunStatus(row[9]),
            started_at=row[10],
            finished_at=row[11],
            row_count_input=row[12],
            row_count_feature=row[13],
            row_count_snapshot=row[14],
            error_code=row[15],
            error_message=row[16],
        )
