from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Iterable

import duckdb

from quant_research.market_calendar.contracts import (
    CalendarDaySnapshot,
    CalendarImportRun,
    CalendarImportStatus,
    CalendarRef,
    CalendarSnapshotSet,
    CalendarSnapshotSetItem,
    MarketCalendarDefinition,
    MarketSession,
)
from quant_research.market_calendar.quality import (
    CalendarQualityIssue,
    CalendarQualityReport,
    CalendarQualitySeverity,
)


class CalendarStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class CalendarSnapshotCommit:
    snapshot: CalendarDaySnapshot
    reused_existing: bool = False


class LocalDuckDBCalendarStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def register_definition(self, definition: MarketCalendarDefinition) -> MarketCalendarDefinition:
        existing = self.get_definition(definition.calendar_id, definition.version)
        if existing is not None:
            if existing.definition_hash != definition.definition_hash:
                raise CalendarStoreError(
                    "DEFINITION_CONFLICT",
                    "calendar id/version already exists with different content",
                )
            return existing
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO market_calendar_definition
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    definition.calendar_id,
                    definition.version,
                    definition.name,
                    definition.timezone,
                    definition.definition_hash,
                    datetime.now(UTC).isoformat(),
                ],
            )
        return definition

    def get_definition(self, calendar_id: str, version: str) -> MarketCalendarDefinition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT calendar_id, calendar_version, name, timezone
                FROM market_calendar_definition
                WHERE calendar_id = ? AND calendar_version = ?
                """,
                [calendar_id, version],
            ).fetchone()
        return MarketCalendarDefinition(*row) if row else None

    def find_committed_import(self, fingerprint: str) -> CalendarImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, calendar_id, calendar_version, calendar_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, session_count, issue_count,
                       error_code, error_message
                FROM market_calendar_import_run
                WHERE import_fingerprint = ? AND status = 'COMMITTED'
                ORDER BY finished_at DESC LIMIT 1
                """,
                [fingerprint],
            ).fetchone()
        return self._row_to_import(row) if row else None

    def get_import_run(self, import_run_id: str) -> CalendarImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, calendar_id, calendar_version, calendar_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, session_count, issue_count,
                       error_code, error_message
                FROM market_calendar_import_run WHERE import_run_id = ?
                """,
                [import_run_id],
            ).fetchone()
        return self._row_to_import(row) if row else None

    def find_snapshot(
        self,
        calendar_id: str,
        calendar_version: str,
        calendar_date: date,
    ) -> CalendarDaySnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id FROM market_calendar_day
                WHERE calendar_id = ? AND calendar_version = ? AND calendar_date = ?
                """,
                [calendar_id, calendar_version, calendar_date],
            ).fetchone()
        return self.get_snapshot(row[0]) if row else None

    def get_snapshot(self, snapshot_id: str) -> CalendarDaySnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id, calendar_id, calendar_version, calendar_date,
                       is_trading_day, known_at, source_data_cutoff, definition_hash,
                       content_hash, source_ref, source_file_hash, sessions_json
                FROM market_calendar_day WHERE snapshot_id = ?
                """,
                [snapshot_id],
            ).fetchone()
        if row is None:
            return None
        sessions = tuple(
            MarketSession(
                session_id=item["session_id"],
                start_time=time.fromisoformat(item["start_time"]) if item["start_time"] else None,
                end_time=time.fromisoformat(item["end_time"]) if item["end_time"] else None,
                session_kind=item["session_kind"],
                declared_calendar_date=(
                    date.fromisoformat(item["declared_calendar_date"])
                    if item["declared_calendar_date"]
                    else None
                ),
                source_row_id=item["source_row_id"],
                raw_ref=item["raw_ref"],
            )
            for item in json.loads(row[11])
        )
        return CalendarDaySnapshot(
            snapshot_id=row[0],
            calendar_id=row[1],
            calendar_version=row[2],
            calendar_date=row[3],
            is_trading_day=row[4],
            known_at=datetime.fromisoformat(row[5]),
            source_data_cutoff=datetime.fromisoformat(row[6]),
            definition_hash=row[7],
            content_hash=row[8],
            source_ref=row[9],
            source_file_hash=row[10],
            sessions=sessions,
        )

    def commit_snapshot(
        self,
        run: CalendarImportRun,
        snapshot: CalendarDaySnapshot,
        report: CalendarQualityReport,
    ) -> CalendarSnapshotCommit:
        existing = self.find_snapshot(
            snapshot.calendar_id,
            snapshot.calendar_version,
            snapshot.calendar_date,
        )
        if existing is not None and existing.content_hash != snapshot.content_hash:
            raise CalendarStoreError(
                "IMMUTABLE_PARTITION_CONFLICT",
                "calendar date already exists with different content",
            )
        committed = existing or snapshot
        committed_run = replace(
            run,
            status=CalendarImportStatus.COMMITTED,
            finished_at=datetime.now(UTC),
            snapshot_id=committed.snapshot_id,
            session_count=len(committed.sessions),
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
                        INSERT INTO market_calendar_day VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            snapshot.snapshot_id,
                            snapshot.calendar_id,
                            snapshot.calendar_version,
                            snapshot.calendar_date,
                            snapshot.is_trading_day,
                            snapshot.known_at.isoformat(),
                            snapshot.source_data_cutoff.isoformat(),
                            snapshot.definition_hash,
                            snapshot.content_hash,
                            snapshot.source_ref,
                            snapshot.source_file_hash,
                            self._sessions_json(snapshot.sessions),
                            run.import_run_id,
                            datetime.now(UTC).isoformat(),
                        ],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return CalendarSnapshotCommit(committed, existing is not None)

    def fail_import(
        self,
        run: CalendarImportRun,
        report: CalendarQualityReport,
        *,
        error_code: str,
        error_message: str,
        row_count_raw: int = 0,
    ) -> CalendarImportRun:
        failed = replace(
            run,
            status=CalendarImportStatus.FAILED,
            finished_at=datetime.now(UTC),
            row_count_raw=row_count_raw,
            issue_count=report.issue_count,
            error_code=error_code,
            error_message=error_message,
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

    def list_quality_issues(self, import_run_id: str) -> list[CalendarQualityIssue]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT issue_id, import_run_id, issue_code, severity, message,
                       session_id, source_row_id, raw_ref
                FROM market_calendar_quality_issue
                WHERE import_run_id = ? ORDER BY issue_id
                """,
                [import_run_id],
            ).fetchall()
        return [
            CalendarQualityIssue(
                row[0], row[1], row[2], CalendarQualitySeverity(row[3]),
                row[4], row[5], row[6], row[7]
            )
            for row in rows
        ]

    def create_snapshot_set(
        self,
        *,
        calendar_id: str,
        calendar_version: str,
        calendar_dates: Iterable[date],
    ) -> CalendarSnapshotSet:
        requested = tuple(sorted(set(calendar_dates)))
        if not requested:
            raise CalendarStoreError("EMPTY_SNAPSHOT_SET", "calendar_dates must not be empty")
        definition = self.get_definition(calendar_id, calendar_version)
        if definition is None:
            raise CalendarStoreError("UNKNOWN_CALENDAR", "calendar definition does not exist")
        snapshots = [self.find_snapshot(calendar_id, calendar_version, value) for value in requested]
        missing = [value for value, snapshot in zip(requested, snapshots, strict=True) if snapshot is None]
        if missing:
            rendered = ", ".join(value.isoformat() for value in missing)
            raise CalendarStoreError("MISSING_SNAPSHOT", f"missing calendar snapshots: {rendered}")
        snapshot_set = CalendarSnapshotSet.create(
            calendar_id,
            calendar_version,
            definition.definition_hash,
            tuple(
                CalendarSnapshotSetItem(item.calendar_date, item.snapshot_id, item.content_hash)
                for item in snapshots
                if item is not None
            ),
        )
        existing = self.get_snapshot_set(snapshot_set.snapshot_set_id)
        if existing is not None:
            return existing
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    INSERT INTO market_calendar_snapshot_set_manifest
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        snapshot_set.snapshot_set_id,
                        snapshot_set.calendar_id,
                        snapshot_set.calendar_version,
                        snapshot_set.definition_hash,
                        snapshot_set.date_start,
                        snapshot_set.date_end,
                        snapshot_set.snapshot_set_hash,
                        snapshot_set.created_at.isoformat(),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO market_calendar_snapshot_set_item VALUES (?, ?, ?, ?)
                    """,
                    [
                        [snapshot_set.snapshot_set_id, item.calendar_date, item.snapshot_id, item.content_hash]
                        for item in snapshot_set.items
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return snapshot_set

    def read_snapshot_set(self, ref: CalendarRef | str) -> CalendarSnapshotSet:
        calendar_ref = CalendarRef.parse(ref)
        result = self.get_snapshot_set(calendar_ref.snapshot_set_id)
        if result is None:
            raise CalendarStoreError("UNKNOWN_SNAPSHOT_SET", "calendar snapshot set does not exist")
        return result

    def get_snapshot_set(self, snapshot_set_id: str) -> CalendarSnapshotSet | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_set_id, calendar_id, calendar_version, definition_hash,
                       date_start, date_end, snapshot_set_hash, created_at
                FROM market_calendar_snapshot_set_manifest WHERE snapshot_set_id = ?
                """,
                [snapshot_set_id],
            ).fetchone()
            if row is None:
                return None
            items = conn.execute(
                """
                SELECT calendar_date, snapshot_id, content_hash
                FROM market_calendar_snapshot_set_item
                WHERE snapshot_set_id = ? ORDER BY calendar_date
                """,
                [snapshot_set_id],
            ).fetchall()
        return CalendarSnapshotSet(
            snapshot_set_id=row[0],
            calendar_id=row[1],
            calendar_version=row[2],
            definition_hash=row[3],
            date_start=row[4],
            date_end=row[5],
            snapshot_set_hash=row[6],
            items=tuple(CalendarSnapshotSetItem(*item) for item in items),
            created_at=datetime.fromisoformat(row[7]),
        )

    def _initialize(self) -> None:
        statements = [
            """CREATE TABLE IF NOT EXISTS market_calendar_definition (
                calendar_id VARCHAR, calendar_version VARCHAR, name VARCHAR, timezone VARCHAR,
                definition_hash VARCHAR, created_at VARCHAR,
                PRIMARY KEY (calendar_id, calendar_version))""",
            """CREATE TABLE IF NOT EXISTS market_calendar_import_run (
                import_run_id VARCHAR PRIMARY KEY, source_id VARCHAR, calendar_id VARCHAR,
                calendar_version VARCHAR, calendar_date DATE, source_file_hash VARCHAR,
                import_fingerprint VARCHAR, status VARCHAR, started_at VARCHAR, finished_at VARCHAR,
                snapshot_id VARCHAR, row_count_raw BIGINT, session_count BIGINT, issue_count BIGINT,
                error_code VARCHAR, error_message VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS market_calendar_day (
                snapshot_id VARCHAR PRIMARY KEY, calendar_id VARCHAR, calendar_version VARCHAR,
                calendar_date DATE, is_trading_day BOOLEAN, known_at VARCHAR,
                source_data_cutoff VARCHAR, definition_hash VARCHAR, content_hash VARCHAR,
                source_ref VARCHAR, source_file_hash VARCHAR, sessions_json VARCHAR,
                import_run_id VARCHAR, created_at VARCHAR,
                UNIQUE (calendar_id, calendar_version, calendar_date))""",
            """CREATE TABLE IF NOT EXISTS market_calendar_quality_issue (
                issue_id VARCHAR PRIMARY KEY, import_run_id VARCHAR, issue_code VARCHAR,
                severity VARCHAR, message VARCHAR, session_id VARCHAR,
                source_row_id VARCHAR, raw_ref VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS market_calendar_snapshot_set_manifest (
                snapshot_set_id VARCHAR PRIMARY KEY, calendar_id VARCHAR, calendar_version VARCHAR,
                definition_hash VARCHAR, date_start DATE, date_end DATE,
                snapshot_set_hash VARCHAR, created_at VARCHAR)""",
            """CREATE TABLE IF NOT EXISTS market_calendar_snapshot_set_item (
                snapshot_set_id VARCHAR, calendar_date DATE, snapshot_id VARCHAR,
                content_hash VARCHAR, PRIMARY KEY (snapshot_set_id, calendar_date))""",
        ]
        with self._connect() as conn:
            for statement in statements:
                conn.execute(statement)

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _replace_import(self, conn, run: CalendarImportRun) -> None:
        conn.execute("DELETE FROM market_calendar_import_run WHERE import_run_id = ?", [run.import_run_id])
        conn.execute(
            """
            INSERT INTO market_calendar_import_run VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.import_run_id, run.source_id, run.calendar_id, run.calendar_version,
                run.calendar_date, run.source_file_hash, run.import_fingerprint, run.status.value,
                run.started_at.isoformat(), run.finished_at.isoformat() if run.finished_at else None,
                run.snapshot_id, run.row_count_raw, run.session_count, run.issue_count,
                run.error_code, run.error_message,
            ],
        )

    def _replace_issues(
        self,
        conn,
        import_run_id: str,
        issues: Iterable[CalendarQualityIssue],
    ) -> None:
        conn.execute("DELETE FROM market_calendar_quality_issue WHERE import_run_id = ?", [import_run_id])
        rows = list(issues)
        if rows:
            conn.executemany(
                "INSERT INTO market_calendar_quality_issue VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    [
                        item.issue_id, item.import_run_id, item.issue_code, item.severity.value,
                        item.message, item.session_id, item.source_row_id, item.raw_ref,
                    ]
                    for item in rows
                ],
            )

    def _row_to_import(self, row) -> CalendarImportRun:
        return CalendarImportRun(
            import_run_id=row[0], source_id=row[1], calendar_id=row[2], calendar_version=row[3],
            calendar_date=row[4], source_file_hash=row[5], import_fingerprint=row[6],
            status=CalendarImportStatus(row[7]), started_at=datetime.fromisoformat(row[8]),
            finished_at=datetime.fromisoformat(row[9]) if row[9] else None,
            snapshot_id=row[10], row_count_raw=row[11], session_count=row[12],
            issue_count=row[13], error_code=row[14], error_message=row[15],
        )

    def _sessions_json(self, sessions: tuple[MarketSession, ...]) -> str:
        return json.dumps(
            [
                {
                    **session.canonical_payload,
                    "declared_calendar_date": (
                        session.declared_calendar_date.isoformat()
                        if session.declared_calendar_date
                        else None
                    ),
                    "source_row_id": session.source_row_id,
                    "raw_ref": session.raw_ref,
                }
                for session in sessions
            ],
            sort_keys=True,
        )
