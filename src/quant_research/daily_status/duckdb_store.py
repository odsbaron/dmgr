from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Iterable

import duckdb

from quant_research.contracts.bar import AssetClass
from quant_research.daily_status.contracts import (
    BarExpectation,
    DailyStatusDefinition,
    DailyStatusRef,
    DailyStatusSnapshot,
    InstrumentDailyStatus,
    LocalTimeInterval,
    MarketState,
    StatusImportRun,
    StatusImportStatus,
    StatusSnapshotSet,
    StatusSnapshotSetItem,
)
from quant_research.daily_status.quality import (
    StatusQualityIssue,
    StatusQualityReport,
    StatusQualitySeverity,
)


class DailyStatusStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class DailyStatusSnapshotCommit:
    snapshot: DailyStatusSnapshot
    reused_existing: bool = False


class LocalDuckDBDailyStatusStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def register_definition(self, definition: DailyStatusDefinition) -> DailyStatusDefinition:
        existing = self.get_definition(definition.status_id, definition.version)
        if existing is not None:
            if existing.definition_hash != definition.definition_hash:
                raise DailyStatusStoreError(
                    "DEFINITION_CONFLICT",
                    "status id/version already exists with different content",
                )
            return existing
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_status_definition VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    definition.status_id,
                    definition.version,
                    definition.name,
                    definition.asset_class.value,
                    definition.calendar_id,
                    definition.calendar_version,
                    definition.timezone,
                    definition.definition_hash,
                    datetime.now(UTC).isoformat(),
                ],
            )
        return definition

    def get_definition(self, status_id: str, version: str) -> DailyStatusDefinition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status_id, status_version, name, asset_class,
                       calendar_id, calendar_version, timezone
                FROM daily_status_definition
                WHERE status_id = ? AND status_version = ?
                """,
                [status_id, version],
            ).fetchone()
        if row is None:
            return None
        return DailyStatusDefinition(
            status_id=row[0], version=row[1], name=row[2], asset_class=AssetClass(row[3]),
            calendar_id=row[4], calendar_version=row[5], timezone=row[6],
        )

    def find_committed_import(self, fingerprint: str) -> StatusImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, status_id, status_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, row_count_status, issue_count,
                       error_code, error_message
                FROM daily_status_import_run
                WHERE import_fingerprint = ? AND status = 'COMMITTED'
                ORDER BY finished_at DESC LIMIT 1
                """,
                [fingerprint],
            ).fetchone()
        return self._row_to_import(row) if row else None

    def get_import_run(self, import_run_id: str) -> StatusImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, status_id, status_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, row_count_status, issue_count,
                       error_code, error_message
                FROM daily_status_import_run WHERE import_run_id = ?
                """,
                [import_run_id],
            ).fetchone()
        return self._row_to_import(row) if row else None

    def find_snapshot(
        self,
        status_id: str,
        status_version: str,
        trading_date: date,
    ) -> DailyStatusSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id FROM daily_status_snapshot
                WHERE status_id = ? AND status_version = ? AND trading_date = ?
                """,
                [status_id, status_version, trading_date],
            ).fetchone()
        return self.get_snapshot(row[0]) if row else None

    def get_snapshot(self, snapshot_id: str) -> DailyStatusSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id, status_id, status_version, trading_date, known_at,
                       source_data_cutoff, definition_hash, content_hash,
                       source_ref, source_file_hash
                FROM daily_status_snapshot WHERE snapshot_id = ?
                """,
                [snapshot_id],
            ).fetchone()
        if row is None:
            return None
        return DailyStatusSnapshot(
            snapshot_id=row[0], status_id=row[1], status_version=row[2], trading_date=row[3],
            known_at=datetime.fromisoformat(row[4]), source_data_cutoff=datetime.fromisoformat(row[5]),
            definition_hash=row[6], content_hash=row[7], source_ref=row[8], source_file_hash=row[9],
            statuses=tuple(self.read_statuses(row[0])),
        )

    def read_statuses(self, snapshot_id: str) -> list[InstrumentDailyStatus]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT instrument_id, market_state, bar_expectation, custom_intervals_json,
                       declared_trading_date, source_row_id, raw_ref
                FROM instrument_daily_status
                WHERE snapshot_id = ? ORDER BY instrument_id
                """,
                [snapshot_id],
            ).fetchall()
        return [
            InstrumentDailyStatus(
                instrument_id=row[0], market_state=MarketState(row[1]),
                bar_expectation=BarExpectation(row[2]),
                custom_intervals=tuple(
                    LocalTimeInterval(
                        time.fromisoformat(item["start_time"]),
                        time.fromisoformat(item["end_time"]),
                    )
                    for item in json.loads(row[3])
                ),
                declared_trading_date=row[4], source_row_id=row[5], raw_ref=row[6],
            )
            for row in rows
        ]

    def commit_snapshot(
        self,
        run: StatusImportRun,
        snapshot: DailyStatusSnapshot,
        report: StatusQualityReport,
    ) -> DailyStatusSnapshotCommit:
        existing = self.find_snapshot(snapshot.status_id, snapshot.status_version, snapshot.trading_date)
        if existing is not None and existing.content_hash != snapshot.content_hash:
            raise DailyStatusStoreError(
                "IMMUTABLE_PARTITION_CONFLICT",
                "daily status date already exists with different content",
            )
        committed = existing or snapshot
        committed_run = replace(
            run, status=StatusImportStatus.COMMITTED, finished_at=datetime.now(UTC),
            snapshot_id=committed.snapshot_id, row_count_status=len(committed.statuses),
            issue_count=report.issue_count,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import(conn, committed_run)
                self._replace_issues(conn, run.import_run_id, report.issues)
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO daily_status_snapshot VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            snapshot.snapshot_id, snapshot.status_id, snapshot.status_version,
                            snapshot.trading_date, snapshot.known_at.isoformat(),
                            snapshot.source_data_cutoff.isoformat(), snapshot.definition_hash,
                            snapshot.content_hash, snapshot.source_ref, snapshot.source_file_hash,
                            run.import_run_id, datetime.now(UTC).isoformat(),
                        ],
                    )
                    self._insert_statuses(conn, snapshot)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return DailyStatusSnapshotCommit(committed, existing is not None)

    def fail_import(
        self,
        run: StatusImportRun,
        report: StatusQualityReport,
        *,
        error_code: str,
        error_message: str,
        row_count_raw: int = 0,
    ) -> StatusImportRun:
        failed = replace(
            run, status=StatusImportStatus.FAILED, finished_at=datetime.now(UTC),
            row_count_raw=row_count_raw, issue_count=report.issue_count,
            error_code=error_code, error_message=error_message,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import(conn, failed)
                self._replace_issues(conn, run.import_run_id, report.issues)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return failed

    def list_quality_issues(self, import_run_id: str) -> list[StatusQualityIssue]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT issue_id, import_run_id, issue_code, severity, message,
                       instrument_id, source_row_id, raw_ref
                FROM daily_status_quality_issue
                WHERE import_run_id = ? ORDER BY issue_id
                """,
                [import_run_id],
            ).fetchall()
        return [
            StatusQualityIssue(
                row[0], row[1], row[2], StatusQualitySeverity(row[3]),
                row[4], row[5], row[6], row[7],
            )
            for row in rows
        ]

    def create_snapshot_set(
        self,
        *,
        status_id: str,
        status_version: str,
        trading_dates: Iterable[date],
    ) -> StatusSnapshotSet:
        requested = tuple(sorted(set(trading_dates)))
        if not requested:
            raise DailyStatusStoreError("EMPTY_SNAPSHOT_SET", "trading_dates must not be empty")
        definition = self.get_definition(status_id, status_version)
        if definition is None:
            raise DailyStatusStoreError("UNKNOWN_STATUS", "daily status definition does not exist")
        snapshots = [self.find_snapshot(status_id, status_version, value) for value in requested]
        missing = [value for value, snapshot in zip(requested, snapshots, strict=True) if snapshot is None]
        if missing:
            rendered = ", ".join(value.isoformat() for value in missing)
            raise DailyStatusStoreError("MISSING_SNAPSHOT", f"missing status snapshots: {rendered}")
        snapshot_set = StatusSnapshotSet.create(
            status_id, status_version, definition.definition_hash,
            tuple(
                StatusSnapshotSetItem(item.trading_date, item.snapshot_id, item.content_hash)
                for item in snapshots if item is not None
            ),
        )
        existing = self.get_snapshot_set(snapshot_set.snapshot_set_id)
        if existing is not None:
            return existing
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "INSERT INTO daily_status_snapshot_set_manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        snapshot_set.snapshot_set_id, snapshot_set.status_id,
                        snapshot_set.status_version, snapshot_set.definition_hash,
                        snapshot_set.date_start, snapshot_set.date_end,
                        snapshot_set.snapshot_set_hash, snapshot_set.created_at.isoformat(),
                    ],
                )
                conn.executemany(
                    "INSERT INTO daily_status_snapshot_set_item VALUES (?, ?, ?, ?)",
                    [
                        [snapshot_set.snapshot_set_id, item.trading_date, item.snapshot_id, item.content_hash]
                        for item in snapshot_set.items
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return snapshot_set

    def read_snapshot_set(self, ref: DailyStatusRef | str) -> StatusSnapshotSet:
        status_ref = DailyStatusRef.parse(ref)
        result = self.get_snapshot_set(status_ref.snapshot_set_id)
        if result is None:
            raise DailyStatusStoreError("UNKNOWN_SNAPSHOT_SET", "status snapshot set does not exist")
        return result

    def get_snapshot_set(self, snapshot_set_id: str) -> StatusSnapshotSet | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_set_id, status_id, status_version, definition_hash,
                       date_start, date_end, snapshot_set_hash, created_at
                FROM daily_status_snapshot_set_manifest WHERE snapshot_set_id = ?
                """,
                [snapshot_set_id],
            ).fetchone()
            if row is None:
                return None
            items = conn.execute(
                """
                SELECT trading_date, snapshot_id, content_hash
                FROM daily_status_snapshot_set_item
                WHERE snapshot_set_id = ? ORDER BY trading_date
                """,
                [snapshot_set_id],
            ).fetchall()
        return StatusSnapshotSet(
            snapshot_set_id=row[0], status_id=row[1], status_version=row[2],
            definition_hash=row[3], date_start=row[4], date_end=row[5],
            snapshot_set_hash=row[6], items=tuple(StatusSnapshotSetItem(*item) for item in items),
            created_at=datetime.fromisoformat(row[7]),
        )

    def _initialize(self) -> None:
        statements = [
            """CREATE TABLE IF NOT EXISTS daily_status_definition (
                status_id VARCHAR, status_version VARCHAR, name VARCHAR, asset_class VARCHAR,
                calendar_id VARCHAR, calendar_version VARCHAR, timezone VARCHAR,
                definition_hash VARCHAR, created_at VARCHAR,
                PRIMARY KEY (status_id, status_version))""",
            """CREATE TABLE IF NOT EXISTS daily_status_import_run (
                import_run_id VARCHAR PRIMARY KEY, source_id VARCHAR, status_id VARCHAR,
                status_version VARCHAR, trading_date DATE, source_file_hash VARCHAR,
                import_fingerprint VARCHAR, status VARCHAR, started_at VARCHAR, finished_at VARCHAR,
                snapshot_id VARCHAR, row_count_raw BIGINT, row_count_status BIGINT, issue_count BIGINT,
                error_code VARCHAR, error_message VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS daily_status_snapshot (
                snapshot_id VARCHAR PRIMARY KEY, status_id VARCHAR, status_version VARCHAR,
                trading_date DATE, known_at VARCHAR, source_data_cutoff VARCHAR,
                definition_hash VARCHAR, content_hash VARCHAR, source_ref VARCHAR,
                source_file_hash VARCHAR, import_run_id VARCHAR, created_at VARCHAR,
                UNIQUE (status_id, status_version, trading_date))""",
            """CREATE TABLE IF NOT EXISTS instrument_daily_status (
                snapshot_id VARCHAR, instrument_id VARCHAR, market_state VARCHAR,
                bar_expectation VARCHAR, custom_intervals_json VARCHAR,
                declared_trading_date DATE, source_row_id VARCHAR, raw_ref VARCHAR,
                PRIMARY KEY (snapshot_id, instrument_id))""",
            """CREATE TABLE IF NOT EXISTS daily_status_quality_issue (
                issue_id VARCHAR PRIMARY KEY, import_run_id VARCHAR, issue_code VARCHAR,
                severity VARCHAR, message VARCHAR, instrument_id VARCHAR,
                source_row_id VARCHAR, raw_ref VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS daily_status_snapshot_set_manifest (
                snapshot_set_id VARCHAR PRIMARY KEY, status_id VARCHAR, status_version VARCHAR,
                definition_hash VARCHAR, date_start DATE, date_end DATE,
                snapshot_set_hash VARCHAR, created_at VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS daily_status_snapshot_set_item (
                snapshot_set_id VARCHAR, trading_date DATE, snapshot_id VARCHAR,
                content_hash VARCHAR, PRIMARY KEY (snapshot_set_id, trading_date))""",
        ]
        with self._connect() as conn:
            for statement in statements:
                conn.execute(statement)

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _replace_import(self, conn, run: StatusImportRun) -> None:
        conn.execute("DELETE FROM daily_status_import_run WHERE import_run_id = ?", [run.import_run_id])
        conn.execute(
            "INSERT INTO daily_status_import_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run.import_run_id, run.source_id, run.status_id, run.status_version,
                run.trading_date, run.source_file_hash, run.import_fingerprint, run.status.value,
                run.started_at.isoformat(), run.finished_at.isoformat() if run.finished_at else None,
                run.snapshot_id, run.row_count_raw, run.row_count_status, run.issue_count,
                run.error_code, run.error_message,
            ],
        )

    def _replace_issues(
        self,
        conn,
        import_run_id: str,
        issues: Iterable[StatusQualityIssue],
    ) -> None:
        conn.execute("DELETE FROM daily_status_quality_issue WHERE import_run_id = ?", [import_run_id])
        rows = list(issues)
        if rows:
            conn.executemany(
                "INSERT INTO daily_status_quality_issue VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    [
                        item.issue_id, item.import_run_id, item.issue_code, item.severity.value,
                        item.message, item.instrument_id, item.source_row_id, item.raw_ref,
                    ]
                    for item in rows
                ],
            )

    def _insert_statuses(self, conn, snapshot: DailyStatusSnapshot) -> None:
        conn.executemany(
            "INSERT INTO instrument_daily_status VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                [
                    snapshot.snapshot_id, item.instrument_id, item.market_state.value,
                    item.bar_expectation.value,
                    json.dumps([interval.canonical_payload for interval in item.custom_intervals], sort_keys=True),
                    item.declared_trading_date, item.source_row_id, item.raw_ref,
                ]
                for item in snapshot.statuses
            ],
        )

    def _row_to_import(self, row) -> StatusImportRun:
        return StatusImportRun(
            import_run_id=row[0], source_id=row[1], status_id=row[2], status_version=row[3],
            trading_date=row[4], source_file_hash=row[5], import_fingerprint=row[6],
            status=StatusImportStatus(row[7]), started_at=datetime.fromisoformat(row[8]),
            finished_at=datetime.fromisoformat(row[9]) if row[9] else None,
            snapshot_id=row[10], row_count_raw=row[11], row_count_status=row[12],
            issue_count=row[13], error_code=row[14], error_message=row[15],
        )
