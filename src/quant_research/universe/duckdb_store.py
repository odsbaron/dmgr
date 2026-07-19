from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Iterable

import duckdb

from quant_research.contracts.bar import AssetClass
from quant_research.universe.contracts import (
    UniverseConstructionMode,
    UniverseDefinition,
    UniverseImportRun,
    UniverseImportStatus,
    UniverseMember,
    UniverseRef,
    UniverseSnapshot,
    UniverseSnapshotSet,
    UniverseSnapshotSetItem,
)
from quant_research.universe.quality import (
    UniverseQualityIssue,
    UniverseQualityReport,
    UniverseQualitySeverity,
)


class UniverseStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class UniverseSnapshotCommit:
    snapshot: UniverseSnapshot
    reused_existing: bool


class LocalDuckDBUniverseStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def register_definition(self, definition: UniverseDefinition) -> UniverseDefinition:
        existing = self.get_definition(definition.universe_id, definition.version)
        if existing is not None:
            if existing.definition_hash != definition.definition_hash:
                raise UniverseStoreError(
                    "DEFINITION_CONFLICT",
                    "universe definition id/version already exists with different content",
                )
            return existing
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO universe_definition (
                    universe_id, universe_version, name, asset_class, calendar_id,
                    timezone, selection_cutoff_time, construction_mode, definition_hash,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    definition.universe_id,
                    definition.version,
                    definition.name,
                    definition.asset_class.value,
                    definition.calendar_id,
                    definition.timezone,
                    definition.selection_cutoff_time.isoformat(),
                    definition.construction_mode.value,
                    definition.definition_hash,
                    datetime.now(UTC).isoformat(),
                ],
            )
        return definition

    def get_definition(self, universe_id: str, version: str) -> UniverseDefinition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT universe_id, universe_version, name, asset_class, calendar_id,
                       timezone, selection_cutoff_time, construction_mode
                FROM universe_definition
                WHERE universe_id = ? AND universe_version = ?
                """,
                [universe_id, version],
            ).fetchone()
        if row is None:
            return None
        return UniverseDefinition(
            universe_id=row[0],
            version=row[1],
            name=row[2],
            asset_class=AssetClass(row[3]),
            calendar_id=row[4],
            timezone=row[5],
            selection_cutoff_time=time.fromisoformat(row[6]),
            construction_mode=UniverseConstructionMode(row[7]),
        )

    def find_committed_import(self, import_fingerprint: str) -> UniverseImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, universe_id, universe_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, row_count_member, issue_count,
                       error_code, error_message
                FROM universe_import_run
                WHERE import_fingerprint = ? AND status = ?
                ORDER BY finished_at DESC, import_run_id DESC
                LIMIT 1
                """,
                [import_fingerprint, UniverseImportStatus.COMMITTED.value],
            ).fetchone()
        return self._row_to_import_run(row) if row else None

    def get_import_run(self, import_run_id: str) -> UniverseImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, universe_id, universe_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       snapshot_id, row_count_raw, row_count_member, issue_count,
                       error_code, error_message
                FROM universe_import_run
                WHERE import_run_id = ?
                """,
                [import_run_id],
            ).fetchone()
        return self._row_to_import_run(row) if row else None

    def find_snapshot(
        self,
        universe_id: str,
        universe_version: str,
        trading_date: date,
    ) -> UniverseSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id
                FROM universe_snapshot
                WHERE universe_id = ? AND universe_version = ? AND trading_date = ?
                  AND status = 'COMMITTED'
                """,
                [universe_id, universe_version, trading_date],
            ).fetchone()
        return self.get_snapshot(row[0]) if row else None

    def get_snapshot(self, snapshot_id: str) -> UniverseSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_id, universe_id, universe_version, trading_date, known_at,
                       source_data_cutoff, definition_hash, content_hash, source_ref,
                       source_file_hash
                FROM universe_snapshot
                WHERE snapshot_id = ? AND status = 'COMMITTED'
                """,
                [snapshot_id],
            ).fetchone()
        if row is None:
            return None
        return UniverseSnapshot(
            snapshot_id=row[0],
            universe_id=row[1],
            universe_version=row[2],
            trading_date=row[3],
            known_at=datetime.fromisoformat(row[4]),
            source_data_cutoff=datetime.fromisoformat(row[5]),
            definition_hash=row[6],
            content_hash=row[7],
            source_ref=row[8],
            source_file_hash=row[9],
            members=tuple(self.read_members(row[0])),
        )

    def read_members(self, snapshot_id: str) -> list[UniverseMember]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT instrument_id, weight, member_rank, inclusion_tags_json,
                       source_row_id, raw_ref
                FROM universe_member
                WHERE snapshot_id = ?
                ORDER BY instrument_id
                """,
                [snapshot_id],
            ).fetchall()
        return [
            UniverseMember(
                instrument_id=row[0],
                weight=row[1],
                rank=row[2],
                inclusion_tags=tuple(json.loads(row[3])),
                source_row_id=row[4],
                raw_ref=row[5],
            )
            for row in rows
        ]

    def commit_snapshot(
        self,
        run: UniverseImportRun,
        snapshot: UniverseSnapshot,
        report: UniverseQualityReport,
    ) -> UniverseSnapshotCommit:
        existing = self.find_snapshot(
            snapshot.universe_id,
            snapshot.universe_version,
            snapshot.trading_date,
        )
        if existing is not None and existing.content_hash != snapshot.content_hash:
            raise UniverseStoreError(
                "IMMUTABLE_PARTITION_CONFLICT",
                "daily Universe partition already exists with different content",
            )
        committed_snapshot = existing or snapshot
        committed_run = replace(
            run,
            status=UniverseImportStatus.COMMITTED,
            finished_at=datetime.now(UTC),
            snapshot_id=committed_snapshot.snapshot_id,
            row_count_member=len(committed_snapshot.members),
            issue_count=report.issue_count,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import_run(conn, committed_run)
                self._replace_quality_issues(conn, run.import_run_id, report.issues)
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO universe_snapshot (
                            snapshot_id, universe_id, universe_version, trading_date, known_at,
                            source_data_cutoff, definition_hash, content_hash, source_ref,
                            source_file_hash, member_count, status, import_run_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?, ?)
                        """,
                        [
                            snapshot.snapshot_id,
                            snapshot.universe_id,
                            snapshot.universe_version,
                            snapshot.trading_date,
                            snapshot.known_at.isoformat(),
                            snapshot.source_data_cutoff.isoformat(),
                            snapshot.definition_hash,
                            snapshot.content_hash,
                            snapshot.source_ref,
                            snapshot.source_file_hash,
                            len(snapshot.members),
                            run.import_run_id,
                            datetime.now(UTC).isoformat(),
                        ],
                    )
                    self._insert_members(conn, snapshot)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return UniverseSnapshotCommit(committed_snapshot, reused_existing=existing is not None)

    def fail_import(
        self,
        run: UniverseImportRun,
        report: UniverseQualityReport,
        *,
        error_code: str,
        error_message: str,
        row_count_raw: int = 0,
    ) -> UniverseImportRun:
        failed = replace(
            run,
            status=UniverseImportStatus.FAILED,
            finished_at=datetime.now(UTC),
            row_count_raw=row_count_raw,
            row_count_member=0,
            issue_count=report.issue_count,
            error_code=error_code,
            error_message=error_message,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import_run(conn, failed)
                self._replace_quality_issues(conn, run.import_run_id, report.issues)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return failed

    def list_quality_issues(self, import_run_id: str) -> list[UniverseQualityIssue]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT issue_id, import_run_id, universe_id, universe_version, trading_date,
                       issue_code, severity, message, instrument_id, source_row_id, raw_ref
                FROM universe_quality_issue
                WHERE import_run_id = ?
                ORDER BY issue_id
                """,
                [import_run_id],
            ).fetchall()
        return [
            UniverseQualityIssue(
                issue_id=row[0],
                import_run_id=row[1],
                universe_id=row[2],
                universe_version=row[3],
                trading_date=row[4].isoformat(),
                issue_code=row[5],
                severity=UniverseQualitySeverity(row[6]),
                message=row[7],
                instrument_id=row[8],
                source_row_id=row[9],
                raw_ref=row[10],
            )
            for row in rows
        ]

    def create_snapshot_set(
        self,
        *,
        universe_id: str,
        universe_version: str,
        trading_dates: Iterable[date],
    ) -> UniverseSnapshotSet:
        requested = tuple(sorted(set(trading_dates)))
        if not requested:
            raise UniverseStoreError("EMPTY_SNAPSHOT_SET", "trading_dates must not be empty")
        definition = self.get_definition(universe_id, universe_version)
        if definition is None:
            raise UniverseStoreError("UNKNOWN_UNIVERSE", "universe definition does not exist")
        snapshots: list[UniverseSnapshot] = []
        missing: list[date] = []
        for trading_date in requested:
            snapshot = self.find_snapshot(universe_id, universe_version, trading_date)
            if snapshot is None:
                missing.append(trading_date)
            else:
                snapshots.append(snapshot)
        if missing:
            rendered = ", ".join(value.isoformat() for value in missing)
            raise UniverseStoreError("MISSING_SNAPSHOT", f"missing Universe snapshots: {rendered}")
        snapshot_set = UniverseSnapshotSet.create(
            universe_id=universe_id,
            universe_version=universe_version,
            definition_hash=definition.definition_hash,
            items=tuple(
                UniverseSnapshotSetItem(
                    trading_date=snapshot.trading_date,
                    snapshot_id=snapshot.snapshot_id,
                    content_hash=snapshot.content_hash,
                )
                for snapshot in snapshots
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
                    INSERT INTO universe_snapshot_set_manifest (
                        snapshot_set_id, universe_id, universe_version, definition_hash,
                        date_start, date_end, snapshot_set_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        snapshot_set.snapshot_set_id,
                        snapshot_set.universe_id,
                        snapshot_set.universe_version,
                        snapshot_set.definition_hash,
                        snapshot_set.date_start,
                        snapshot_set.date_end,
                        snapshot_set.snapshot_set_hash,
                        snapshot_set.created_at.isoformat(),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO universe_snapshot_set_item (
                        snapshot_set_id, trading_date, snapshot_id, content_hash
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        [
                            snapshot_set.snapshot_set_id,
                            item.trading_date,
                            item.snapshot_id,
                            item.content_hash,
                        ]
                        for item in snapshot_set.items
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return snapshot_set

    def read_snapshot_set(self, ref: UniverseRef | str) -> UniverseSnapshotSet:
        universe_ref = UniverseRef.parse(ref)
        snapshot_set = self.get_snapshot_set(universe_ref.snapshot_set_id)
        if snapshot_set is None:
            raise UniverseStoreError("UNKNOWN_SNAPSHOT_SET", "Universe snapshot set does not exist")
        return snapshot_set

    def get_snapshot_set(self, snapshot_set_id: str) -> UniverseSnapshotSet | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_set_id, universe_id, universe_version, definition_hash,
                       date_start, date_end, snapshot_set_hash, created_at
                FROM universe_snapshot_set_manifest
                WHERE snapshot_set_id = ?
                """,
                [snapshot_set_id],
            ).fetchone()
            if row is None:
                return None
            item_rows = conn.execute(
                """
                SELECT trading_date, snapshot_id, content_hash
                FROM universe_snapshot_set_item
                WHERE snapshot_set_id = ?
                ORDER BY trading_date
                """,
                [snapshot_set_id],
            ).fetchall()
        return UniverseSnapshotSet(
            snapshot_set_id=row[0],
            universe_id=row[1],
            universe_version=row[2],
            definition_hash=row[3],
            date_start=row[4],
            date_end=row[5],
            snapshot_set_hash=row[6],
            items=tuple(
                UniverseSnapshotSetItem(
                    trading_date=item[0],
                    snapshot_id=item[1],
                    content_hash=item[2],
                )
                for item in item_rows
            ),
            created_at=datetime.fromisoformat(row[7]),
        )

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_definition (
                    universe_id VARCHAR NOT NULL,
                    universe_version VARCHAR NOT NULL,
                    name VARCHAR NOT NULL,
                    asset_class VARCHAR NOT NULL,
                    calendar_id VARCHAR NOT NULL,
                    timezone VARCHAR NOT NULL,
                    selection_cutoff_time VARCHAR NOT NULL,
                    construction_mode VARCHAR NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    PRIMARY KEY (universe_id, universe_version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_import_run (
                    import_run_id VARCHAR PRIMARY KEY,
                    source_id VARCHAR NOT NULL,
                    universe_id VARCHAR NOT NULL,
                    universe_version VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    source_file_hash VARCHAR NOT NULL,
                    import_fingerprint VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR,
                    snapshot_id VARCHAR,
                    row_count_raw BIGINT NOT NULL,
                    row_count_member BIGINT NOT NULL,
                    issue_count BIGINT NOT NULL,
                    error_code VARCHAR,
                    error_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_snapshot (
                    snapshot_id VARCHAR PRIMARY KEY,
                    universe_id VARCHAR NOT NULL,
                    universe_version VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    known_at VARCHAR NOT NULL,
                    source_data_cutoff VARCHAR NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    source_ref VARCHAR NOT NULL,
                    source_file_hash VARCHAR NOT NULL,
                    member_count BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    import_run_id VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    UNIQUE (universe_id, universe_version, trading_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_member (
                    snapshot_id VARCHAR NOT NULL,
                    instrument_id VARCHAR NOT NULL,
                    weight DOUBLE,
                    member_rank BIGINT,
                    inclusion_tags_json VARCHAR NOT NULL,
                    source_row_id VARCHAR,
                    raw_ref VARCHAR,
                    PRIMARY KEY (snapshot_id, instrument_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_quality_issue (
                    issue_id VARCHAR PRIMARY KEY,
                    import_run_id VARCHAR NOT NULL,
                    universe_id VARCHAR NOT NULL,
                    universe_version VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    issue_code VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    message VARCHAR NOT NULL,
                    instrument_id VARCHAR,
                    source_row_id VARCHAR,
                    raw_ref VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_snapshot_set_manifest (
                    snapshot_set_id VARCHAR PRIMARY KEY,
                    universe_id VARCHAR NOT NULL,
                    universe_version VARCHAR NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    date_start DATE NOT NULL,
                    date_end DATE NOT NULL,
                    snapshot_set_hash VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_snapshot_set_item (
                    snapshot_set_id VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    snapshot_id VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    PRIMARY KEY (snapshot_set_id, trading_date)
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _replace_import_run(self, conn, run: UniverseImportRun) -> None:
        conn.execute("DELETE FROM universe_import_run WHERE import_run_id = ?", [run.import_run_id])
        conn.execute(
            """
            INSERT INTO universe_import_run (
                import_run_id, source_id, universe_id, universe_version, trading_date,
                source_file_hash, import_fingerprint, status, started_at, finished_at,
                snapshot_id, row_count_raw, row_count_member, issue_count,
                error_code, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.import_run_id,
                run.source_id,
                run.universe_id,
                run.universe_version,
                run.trading_date,
                run.source_file_hash,
                run.import_fingerprint,
                run.status.value,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.snapshot_id,
                run.row_count_raw,
                run.row_count_member,
                run.issue_count,
                run.error_code,
                run.error_message,
            ],
        )

    def _replace_quality_issues(
        self,
        conn,
        import_run_id: str,
        issues: Iterable[UniverseQualityIssue],
    ) -> None:
        conn.execute("DELETE FROM universe_quality_issue WHERE import_run_id = ?", [import_run_id])
        rows = list(issues)
        if not rows:
            return
        conn.executemany(
            """
            INSERT INTO universe_quality_issue (
                issue_id, import_run_id, universe_id, universe_version, trading_date,
                issue_code, severity, message, instrument_id, source_row_id, raw_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    issue.issue_id,
                    issue.import_run_id,
                    issue.universe_id,
                    issue.universe_version,
                    date.fromisoformat(issue.trading_date),
                    issue.issue_code,
                    issue.severity.value,
                    issue.message,
                    issue.instrument_id,
                    issue.source_row_id,
                    issue.raw_ref,
                ]
                for issue in rows
            ],
        )

    def _insert_members(self, conn, snapshot: UniverseSnapshot) -> None:
        if not snapshot.members:
            return
        conn.executemany(
            """
            INSERT INTO universe_member (
                snapshot_id, instrument_id, weight, member_rank, inclusion_tags_json,
                source_row_id, raw_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    snapshot.snapshot_id,
                    member.instrument_id,
                    member.weight,
                    member.rank,
                    json.dumps(list(member.inclusion_tags), sort_keys=True),
                    member.source_row_id,
                    member.raw_ref,
                ]
                for member in snapshot.members
            ],
        )

    def _row_to_import_run(self, row) -> UniverseImportRun:
        return UniverseImportRun(
            import_run_id=row[0],
            source_id=row[1],
            universe_id=row[2],
            universe_version=row[3],
            trading_date=row[4],
            source_file_hash=row[5],
            import_fingerprint=row[6],
            status=UniverseImportStatus(row[7]),
            started_at=datetime.fromisoformat(row[8]),
            finished_at=datetime.fromisoformat(row[9]) if row[9] else None,
            snapshot_id=row[10],
            row_count_raw=row[11],
            row_count_member=row[12],
            issue_count=row[13],
            error_code=row[14],
            error_message=row[15],
        )
