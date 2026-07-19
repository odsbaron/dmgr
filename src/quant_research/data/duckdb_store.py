from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun, ImportStatus
from quant_research.contracts.quality import QualityIssue, QualityReport, Severity
from quant_research.contracts.refs import DataRef
from quant_research.contracts.source import BarTimestampConvention
from quant_research.data.partition_contracts import (
    MarketDataImportRun,
    MarketDataPartition,
    MarketDataRef,
    MarketDataSnapshotSet,
    MarketDataSnapshotSetItem,
    MarketDatasetDefinition,
)


_BAR_COLUMNS = (
    "dataset_id",
    "symbol",
    "exchange",
    "asset_class",
    "freq",
    "trading_date",
    "bar_start_time",
    "bar_end_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "adjustment",
    "source",
    "source_run_id",
    "source_row_id",
    "raw_ref",
)

_ALLOWED_BAR_FILTERS = {
    "dataset_id",
    "symbol",
    "exchange",
    "asset_class",
    "freq",
    "trading_date",
    "adjustment",
    "source",
    "source_run_id",
}


class MarketDataStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class MarketDataPartitionCommit:
    partition: MarketDataPartition
    reused_existing: bool


class LocalDuckDBStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_import(
        self,
        run: ImportRun,
        bars: Iterable[BarRecord],
        report: QualityReport,
    ) -> DataRef:
        bar_list = list(bars)
        committed = replace(
            run,
            status=ImportStatus.COMMITTED,
            finished_at=datetime.now(UTC),
            row_count_raw=len(bar_list),
            row_count_curated=len(bar_list),
            issue_count=report.issue_count,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import_run(conn, committed)
                self._replace_quality_issues(conn, report.issues)
                conn.execute(
                    "DELETE FROM curated_market_bar WHERE source_run_id = ?",
                    [run.import_run_id],
                )
                if bar_list:
                    placeholders = ", ".join(["?"] * len(_BAR_COLUMNS))
                    conn.executemany(
                        f"""
                        INSERT INTO curated_market_bar ({", ".join(_BAR_COLUMNS)})
                        VALUES ({placeholders})
                        """,
                        [self._bar_to_row(bar) for bar in bar_list],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self._data_ref_for_run(committed)

    def register_market_dataset_definition(
        self,
        definition: MarketDatasetDefinition,
    ) -> MarketDatasetDefinition:
        existing = self.get_market_dataset_definition(definition.dataset_id, definition.version)
        if existing is not None:
            if existing.definition_hash != definition.definition_hash:
                raise MarketDataStoreError(
                    "DEFINITION_CONFLICT",
                    "market dataset id/version already exists with different content",
                )
            return existing
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO market_data_definition (
                    dataset_id, dataset_version, name, asset_class, freq, adjustment,
                    calendar_id, timezone, bar_timestamp_convention, definition_hash,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    definition.dataset_id,
                    definition.version,
                    definition.name,
                    definition.asset_class.value,
                    definition.freq.value,
                    definition.adjustment.value,
                    definition.calendar_id,
                    definition.timezone,
                    definition.bar_timestamp_convention.value,
                    definition.definition_hash,
                    datetime.now(UTC).isoformat(),
                ],
            )
        return definition

    def get_market_dataset_definition(
        self,
        dataset_id: str,
        version: str,
    ) -> MarketDatasetDefinition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT dataset_id, dataset_version, name, asset_class, freq, adjustment,
                       calendar_id, timezone, bar_timestamp_convention
                FROM market_data_definition
                WHERE dataset_id = ? AND dataset_version = ?
                """,
                [dataset_id, version],
            ).fetchone()
        if row is None:
            return None
        return MarketDatasetDefinition(
            dataset_id=row[0],
            version=row[1],
            name=row[2],
            asset_class=AssetClass(row[3]),
            freq=Frequency(row[4]),
            adjustment=Adjustment(row[5]),
            calendar_id=row[6],
            timezone=row[7],
            bar_timestamp_convention=BarTimestampConvention(row[8]),
        )

    def find_committed_market_data_import(
        self,
        import_fingerprint: str,
    ) -> MarketDataImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, dataset_id, dataset_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       partition_id, row_count_raw, row_count_curated, issue_count,
                       error_code, error_message
                FROM market_data_import_run
                WHERE import_fingerprint = ? AND status = ?
                ORDER BY finished_at DESC, import_run_id DESC
                LIMIT 1
                """,
                [import_fingerprint, ImportStatus.COMMITTED.value],
            ).fetchone()
        return self._row_to_market_data_import_run(row) if row else None

    def get_market_data_import_run(
        self,
        import_run_id: str,
    ) -> MarketDataImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, source_id, dataset_id, dataset_version, trading_date,
                       source_file_hash, import_fingerprint, status, started_at, finished_at,
                       partition_id, row_count_raw, row_count_curated, issue_count,
                       error_code, error_message
                FROM market_data_import_run
                WHERE import_run_id = ?
                """,
                [import_run_id],
            ).fetchone()
        return self._row_to_market_data_import_run(row) if row else None

    def find_market_data_partition(
        self,
        dataset_id: str,
        dataset_version: str,
        trading_date: date,
    ) -> MarketDataPartition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT partition_id
                FROM market_data_partition
                WHERE dataset_id = ? AND dataset_version = ? AND trading_date = ?
                  AND status = 'COMMITTED'
                """,
                [dataset_id, dataset_version, trading_date],
            ).fetchone()
        return self.get_market_data_partition(row[0]) if row else None

    def get_market_data_partition(
        self,
        partition_id: str,
    ) -> MarketDataPartition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT partition_id, dataset_id, dataset_version, trading_date, known_at,
                       source_data_cutoff, definition_hash, content_hash, source_ref,
                       source_file_hash
                FROM market_data_partition
                WHERE partition_id = ? AND status = 'COMMITTED'
                """,
                [partition_id],
            ).fetchone()
            if row is None:
                return None
            bar_rows = conn.execute(
                f"""
                SELECT {", ".join(_BAR_COLUMNS)}
                FROM curated_market_bar
                WHERE market_data_partition_id = ?
                ORDER BY symbol, bar_start_time
                """,
                [partition_id],
            ).fetchall()
        return MarketDataPartition(
            partition_id=row[0],
            dataset_id=row[1],
            dataset_version=row[2],
            trading_date=row[3],
            known_at=datetime.fromisoformat(row[4]),
            source_data_cutoff=datetime.fromisoformat(row[5]),
            definition_hash=row[6],
            content_hash=row[7],
            source_ref=row[8],
            source_file_hash=row[9],
            bars=tuple(self._row_to_bar(bar_row) for bar_row in bar_rows),
        )

    def commit_market_data_partition(
        self,
        run: MarketDataImportRun,
        partition: MarketDataPartition,
        report: QualityReport,
    ) -> MarketDataPartitionCommit:
        existing = self.find_market_data_partition(
            partition.dataset_id,
            partition.dataset_version,
            partition.trading_date,
        )
        if existing is not None and existing.content_hash != partition.content_hash:
            raise MarketDataStoreError(
                "IMMUTABLE_PARTITION_CONFLICT",
                "daily market-data partition already exists with different content",
            )
        committed_partition = existing or partition
        committed_run = replace(
            run,
            status=ImportStatus.COMMITTED,
            finished_at=datetime.now(UTC),
            partition_id=committed_partition.partition_id,
            row_count_curated=len(committed_partition.bars),
            issue_count=report.issue_count,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_market_data_import_run(conn, committed_run)
                self._replace_quality_issues_for_run(
                    conn,
                    run.import_run_id,
                    report.issues,
                )
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO market_data_partition (
                            partition_id, dataset_id, dataset_version, trading_date, known_at,
                            source_data_cutoff, definition_hash, content_hash, source_ref,
                            source_file_hash, row_count, status, import_run_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?, ?)
                        """,
                        [
                            partition.partition_id,
                            partition.dataset_id,
                            partition.dataset_version,
                            partition.trading_date,
                            partition.known_at.isoformat(),
                            partition.source_data_cutoff.isoformat(),
                            partition.definition_hash,
                            partition.content_hash,
                            partition.source_ref,
                            partition.source_file_hash,
                            len(partition.bars),
                            run.import_run_id,
                            datetime.now(UTC).isoformat(),
                        ],
                    )
                    self._insert_partition_bars(conn, partition)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return MarketDataPartitionCommit(
            partition=committed_partition,
            reused_existing=existing is not None,
        )

    def fail_market_data_import(
        self,
        run: MarketDataImportRun,
        report: QualityReport,
        *,
        error_code: str,
        error_message: str,
        row_count_raw: int = 0,
    ) -> MarketDataImportRun:
        failed = replace(
            run,
            status=ImportStatus.FAILED,
            finished_at=datetime.now(UTC),
            row_count_raw=row_count_raw,
            row_count_curated=0,
            issue_count=report.issue_count,
            error_code=error_code,
            error_message=error_message,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_market_data_import_run(conn, failed)
                self._replace_quality_issues_for_run(
                    conn,
                    run.import_run_id,
                    report.issues,
                )
                conn.execute(
                    "DELETE FROM curated_market_bar WHERE source_run_id = ?",
                    [run.import_run_id],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return failed

    def fail_import(
        self,
        run: ImportRun,
        report: QualityReport,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        failed = replace(
            run,
            status=ImportStatus.FAILED,
            finished_at=datetime.now(UTC),
            row_count_curated=0,
            issue_count=report.issue_count,
            error_code=error_code,
            error_message=error_message,
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._replace_import_run(conn, failed)
                self._replace_quality_issues(conn, report.issues)
                conn.execute(
                    "DELETE FROM curated_market_bar WHERE source_run_id = ?",
                    [run.import_run_id],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def get_import_run(self, import_run_id: str) -> ImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, dataset_id, source_id, freq, adjustment, source_file_hash,
                       status, started_at, finished_at, row_count_raw, row_count_curated,
                       issue_count, error_code, error_message
                FROM import_run
                WHERE import_run_id = ?
                """,
                [import_run_id],
            ).fetchone()
        return self._row_to_import_run(row) if row else None

    def find_committed_import(
        self,
        *,
        dataset_id: str,
        source_id: str,
        freq: Frequency,
        adjustment: Adjustment,
        source_file_hash: str,
    ) -> ImportRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT import_run_id, dataset_id, source_id, freq, adjustment, source_file_hash,
                       status, started_at, finished_at, row_count_raw, row_count_curated,
                       issue_count, error_code, error_message
                FROM import_run
                WHERE dataset_id = ?
                  AND source_id = ?
                  AND freq = ?
                  AND adjustment = ?
                  AND source_file_hash = ?
                  AND status = ?
                ORDER BY finished_at DESC, import_run_id DESC
                LIMIT 1
                """,
                [
                    dataset_id,
                    source_id,
                    freq.value,
                    adjustment.value,
                    source_file_hash,
                    ImportStatus.COMMITTED.value,
                ],
            ).fetchone()
        return self._row_to_import_run(row) if row else None

    def list_quality_issues(self, import_run_id: str) -> list[QualityIssue]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT issue_id, import_run_id, dataset_id, symbol, freq, trading_date,
                       bar_start_time, issue_code, severity, message, raw_ref
                FROM bar_quality_issue
                WHERE import_run_id = ?
                ORDER BY issue_id
                """,
                [import_run_id],
            ).fetchall()
        return [self._row_to_quality_issue(row) for row in rows]

    def create_market_data_snapshot_set(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        trading_dates: Iterable[date],
    ) -> MarketDataSnapshotSet:
        requested = tuple(sorted(set(trading_dates)))
        if not requested:
            raise MarketDataStoreError(
                "EMPTY_SNAPSHOT_SET",
                "trading_dates must not be empty",
            )
        definition = self.get_market_dataset_definition(dataset_id, dataset_version)
        if definition is None:
            raise MarketDataStoreError(
                "UNKNOWN_DATASET_DEFINITION",
                "market dataset definition does not exist",
            )
        partitions: list[MarketDataPartition] = []
        missing: list[date] = []
        for trading_date in requested:
            partition = self.find_market_data_partition(
                dataset_id,
                dataset_version,
                trading_date,
            )
            if partition is None:
                missing.append(trading_date)
            else:
                partitions.append(partition)
        if missing:
            rendered = ", ".join(value.isoformat() for value in missing)
            raise MarketDataStoreError(
                "MISSING_PARTITION",
                f"missing market-data partitions: {rendered}",
            )
        snapshot_set = MarketDataSnapshotSet.create(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            definition_hash=definition.definition_hash,
            items=tuple(
                MarketDataSnapshotSetItem(
                    trading_date=partition.trading_date,
                    partition_id=partition.partition_id,
                    content_hash=partition.content_hash,
                )
                for partition in partitions
            ),
        )
        existing = self.get_market_data_snapshot_set(snapshot_set.snapshot_set_id)
        if existing is not None:
            return existing
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    INSERT INTO market_data_snapshot_set_manifest (
                        snapshot_set_id, dataset_id, dataset_version, definition_hash,
                        date_start, date_end, snapshot_set_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        snapshot_set.snapshot_set_id,
                        snapshot_set.dataset_id,
                        snapshot_set.dataset_version,
                        snapshot_set.definition_hash,
                        snapshot_set.date_start,
                        snapshot_set.date_end,
                        snapshot_set.snapshot_set_hash,
                        snapshot_set.created_at.isoformat(),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO market_data_snapshot_set_item (
                        snapshot_set_id, trading_date, partition_id, content_hash
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        [
                            snapshot_set.snapshot_set_id,
                            item.trading_date,
                            item.partition_id,
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

    def read_market_data_snapshot_set(
        self,
        ref: MarketDataRef | str,
    ) -> MarketDataSnapshotSet:
        market_data_ref = MarketDataRef.parse(ref)
        snapshot_set = self.get_market_data_snapshot_set(market_data_ref.snapshot_set_id)
        if snapshot_set is None:
            raise MarketDataStoreError(
                "UNKNOWN_SNAPSHOT_SET",
                "market-data snapshot set does not exist",
            )
        return snapshot_set

    def get_market_data_snapshot_set(
        self,
        snapshot_set_id: str,
    ) -> MarketDataSnapshotSet | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_set_id, dataset_id, dataset_version, definition_hash,
                       date_start, date_end, snapshot_set_hash, created_at
                FROM market_data_snapshot_set_manifest
                WHERE snapshot_set_id = ?
                """,
                [snapshot_set_id],
            ).fetchone()
            if row is None:
                return None
            item_rows = conn.execute(
                """
                SELECT trading_date, partition_id, content_hash
                FROM market_data_snapshot_set_item
                WHERE snapshot_set_id = ?
                ORDER BY trading_date
                """,
                [snapshot_set_id],
            ).fetchall()
        return MarketDataSnapshotSet(
            snapshot_set_id=row[0],
            dataset_id=row[1],
            dataset_version=row[2],
            definition_hash=row[3],
            date_start=row[4],
            date_end=row[5],
            snapshot_set_hash=row[6],
            items=tuple(
                MarketDataSnapshotSetItem(
                    trading_date=item[0],
                    partition_id=item[1],
                    content_hash=item[2],
                )
                for item in item_rows
            ),
            created_at=datetime.fromisoformat(row[7]),
        )

    def read_bars(self, data_ref: DataRef | str) -> list[BarRecord]:
        ref = DataRef.parse(data_ref) if isinstance(data_ref, str) else data_ref
        if ref.table != "curated_market_bar":
            raise ValueError(f"unsupported DuckDB table for BarRecord reads: {ref.table}")

        if "snapshot_set_id" in ref.filters:
            market_data_ref = MarketDataRef.parse(ref.uri)
            self.read_market_data_snapshot_set(market_data_ref)
            selected = ", ".join(f"b.{column}" for column in _BAR_COLUMNS)
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT {selected}
                    FROM curated_market_bar AS b
                    INNER JOIN market_data_snapshot_set_item AS item
                      ON b.market_data_partition_id = item.partition_id
                    WHERE item.snapshot_set_id = ?
                    ORDER BY b.symbol, b.bar_start_time
                    """,
                    [market_data_ref.snapshot_set_id],
                ).fetchall()
            return [self._row_to_bar(row) for row in rows]

        where_sql, params = self._where_clause(ref.filters)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_BAR_COLUMNS)}
                FROM curated_market_bar
                {where_sql}
                ORDER BY symbol, bar_start_time
                """,
                params,
            ).fetchall()
        return [self._row_to_bar(row) for row in rows]

    def export_bars_to_parquet(
        self,
        data_ref: DataRef | str,
        output_path: str | Path,
    ) -> Path:
        bars = self.read_bars(data_ref)
        if not bars:
            raise ValueError("cannot export an empty curated bar selection")
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([bar.to_dict() for bar in bars]).write_parquet(target)
        return target

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS import_run (
                    import_run_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    source_id VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    adjustment VARCHAR NOT NULL,
                    source_file_hash VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR,
                    row_count_raw BIGINT NOT NULL,
                    row_count_curated BIGINT NOT NULL,
                    issue_count BIGINT NOT NULL,
                    error_code VARCHAR,
                    error_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS curated_market_bar (
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    exchange VARCHAR NOT NULL,
                    asset_class VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    trading_date VARCHAR NOT NULL,
                    bar_start_time VARCHAR NOT NULL,
                    bar_end_time VARCHAR NOT NULL,
                    open VARCHAR NOT NULL,
                    high VARCHAR NOT NULL,
                    low VARCHAR NOT NULL,
                    close VARCHAR NOT NULL,
                    volume VARCHAR NOT NULL,
                    turnover VARCHAR,
                    adjustment VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    source_run_id VARCHAR NOT NULL,
                    source_row_id VARCHAR,
                    raw_ref VARCHAR,
                    market_data_partition_id VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_curated_market_bar_research_lookup
                ON curated_market_bar (dataset_id, freq, trading_date, symbol)
                """
            )
            conn.execute(
                """
                ALTER TABLE curated_market_bar
                ADD COLUMN IF NOT EXISTS market_data_partition_id VARCHAR
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bar_quality_issue (
                    issue_id VARCHAR PRIMARY KEY,
                    import_run_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR,
                    freq VARCHAR,
                    trading_date VARCHAR,
                    bar_start_time VARCHAR,
                    issue_code VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    message VARCHAR NOT NULL,
                    raw_ref VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data_definition (
                    dataset_id VARCHAR NOT NULL,
                    dataset_version VARCHAR NOT NULL,
                    name VARCHAR NOT NULL,
                    asset_class VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    adjustment VARCHAR NOT NULL,
                    calendar_id VARCHAR NOT NULL,
                    timezone VARCHAR NOT NULL,
                    bar_timestamp_convention VARCHAR NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    PRIMARY KEY (dataset_id, dataset_version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data_import_run (
                    import_run_id VARCHAR PRIMARY KEY,
                    source_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    dataset_version VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    source_file_hash VARCHAR NOT NULL,
                    import_fingerprint VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR,
                    partition_id VARCHAR,
                    row_count_raw BIGINT NOT NULL,
                    row_count_curated BIGINT NOT NULL,
                    issue_count BIGINT NOT NULL,
                    error_code VARCHAR,
                    error_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data_partition (
                    partition_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    dataset_version VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    known_at VARCHAR NOT NULL,
                    source_data_cutoff VARCHAR NOT NULL,
                    definition_hash VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    source_ref VARCHAR NOT NULL,
                    source_file_hash VARCHAR NOT NULL,
                    row_count BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    import_run_id VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    UNIQUE (dataset_id, dataset_version, trading_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data_snapshot_set_manifest (
                    snapshot_set_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    dataset_version VARCHAR NOT NULL,
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
                CREATE TABLE IF NOT EXISTS market_data_snapshot_set_item (
                    snapshot_set_id VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    partition_id VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    PRIMARY KEY (snapshot_set_id, trading_date)
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _replace_import_run(self, conn, run: ImportRun) -> None:
        conn.execute("DELETE FROM import_run WHERE import_run_id = ?", [run.import_run_id])
        conn.execute(
            """
            INSERT INTO import_run (
                import_run_id, dataset_id, source_id, freq, adjustment, source_file_hash,
                status, started_at, finished_at, row_count_raw, row_count_curated,
                issue_count, error_code, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.import_run_id,
                run.dataset_id,
                run.source_id,
                run.freq.value,
                run.adjustment.value,
                run.source_file_hash,
                run.status.value,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.row_count_raw,
                run.row_count_curated,
                run.issue_count,
                run.error_code,
                run.error_message,
            ],
        )

    def _replace_market_data_import_run(
        self,
        conn,
        run: MarketDataImportRun,
    ) -> None:
        conn.execute(
            "DELETE FROM market_data_import_run WHERE import_run_id = ?",
            [run.import_run_id],
        )
        conn.execute(
            """
            INSERT INTO market_data_import_run (
                import_run_id, source_id, dataset_id, dataset_version, trading_date,
                source_file_hash, import_fingerprint, status, started_at, finished_at,
                partition_id, row_count_raw, row_count_curated, issue_count,
                error_code, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.import_run_id,
                run.source_id,
                run.dataset_id,
                run.dataset_version,
                run.trading_date,
                run.source_file_hash,
                run.import_fingerprint,
                run.status.value,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.partition_id,
                run.row_count_raw,
                run.row_count_curated,
                run.issue_count,
                run.error_code,
                run.error_message,
            ],
        )

    def _replace_quality_issues(self, conn, issues: Iterable[QualityIssue]) -> None:
        issue_list = list(issues)
        import_run_ids = {issue.import_run_id for issue in issue_list}
        for import_run_id in import_run_ids:
            conn.execute("DELETE FROM bar_quality_issue WHERE import_run_id = ?", [import_run_id])
        if not issue_list:
            return
        conn.executemany(
            """
            INSERT INTO bar_quality_issue (
                issue_id, import_run_id, dataset_id, symbol, freq, trading_date,
                bar_start_time, issue_code, severity, message, raw_ref
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self._quality_issue_to_row(issue) for issue in issue_list],
        )

    def _replace_quality_issues_for_run(
        self,
        conn,
        import_run_id: str,
        issues: Iterable[QualityIssue],
    ) -> None:
        conn.execute("DELETE FROM bar_quality_issue WHERE import_run_id = ?", [import_run_id])
        issue_list = list(issues)
        if not issue_list:
            return
        conn.executemany(
            """
            INSERT INTO bar_quality_issue (
                issue_id, import_run_id, dataset_id, symbol, freq, trading_date,
                bar_start_time, issue_code, severity, message, raw_ref
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self._quality_issue_to_row(issue) for issue in issue_list],
        )

    def _insert_partition_bars(
        self,
        conn,
        partition: MarketDataPartition,
    ) -> None:
        if not partition.bars:
            return
        columns = (*_BAR_COLUMNS, "market_data_partition_id")
        placeholders = ", ".join(["?"] * len(columns))
        conn.executemany(
            f"""
            INSERT INTO curated_market_bar ({", ".join(columns)})
            VALUES ({placeholders})
            """,
            [(*self._bar_to_row(bar), partition.partition_id) for bar in partition.bars],
        )

    def _data_ref_for_run(self, run: ImportRun) -> DataRef:
        return DataRef(
            table="curated_market_bar",
            filters={
                "dataset_id": run.dataset_id,
                "freq": run.freq.value,
                "adjustment": run.adjustment.value,
                "source_run_id": run.import_run_id,
            },
        )

    def _where_clause(self, filters: dict[str, str]) -> tuple[str, list[str]]:
        if not filters:
            return "", []
        unsupported = set(filters) - _ALLOWED_BAR_FILTERS
        if unsupported:
            raise ValueError(f"unsupported curated_market_bar filters: {sorted(unsupported)}")
        clauses = [f"{field} = ?" for field in filters]
        return "WHERE " + " AND ".join(clauses), list(filters.values())

    def _bar_to_row(self, bar: BarRecord) -> tuple[object, ...]:
        return (
            bar.dataset_id,
            bar.symbol,
            bar.exchange,
            bar.asset_class.value,
            bar.freq.value,
            bar.trading_date.isoformat(),
            bar.bar_start_time.isoformat(),
            bar.bar_end_time.isoformat(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.turnover,
            bar.adjustment.value,
            bar.source,
            bar.source_run_id,
            bar.source_row_id,
            bar.raw_ref,
        )

    def _row_to_bar(self, row) -> BarRecord:
        return BarRecord(
            dataset_id=row[0],
            symbol=row[1],
            exchange=row[2],
            asset_class=AssetClass(row[3]),
            freq=Frequency(row[4]),
            trading_date=date.fromisoformat(row[5]),
            bar_start_time=datetime.fromisoformat(row[6]),
            bar_end_time=datetime.fromisoformat(row[7]),
            open=row[8],
            high=row[9],
            low=row[10],
            close=row[11],
            volume=row[12],
            turnover=row[13],
            adjustment=Adjustment(row[14]),
            source=row[15],
            source_run_id=row[16],
            source_row_id=row[17],
            raw_ref=row[18],
        )

    def _row_to_import_run(self, row) -> ImportRun:
        return ImportRun(
            import_run_id=row[0],
            dataset_id=row[1],
            source_id=row[2],
            freq=Frequency(row[3]),
            adjustment=Adjustment(row[4]),
            source_file_hash=row[5],
            status=ImportStatus(row[6]),
            started_at=datetime.fromisoformat(row[7]),
            finished_at=datetime.fromisoformat(row[8]) if row[8] else None,
            row_count_raw=row[9],
            row_count_curated=row[10],
            issue_count=row[11],
            error_code=row[12],
            error_message=row[13],
        )

    def _row_to_market_data_import_run(self, row) -> MarketDataImportRun:
        return MarketDataImportRun(
            import_run_id=row[0],
            source_id=row[1],
            dataset_id=row[2],
            dataset_version=row[3],
            trading_date=row[4],
            source_file_hash=row[5],
            import_fingerprint=row[6],
            status=ImportStatus(row[7]),
            started_at=datetime.fromisoformat(row[8]),
            finished_at=datetime.fromisoformat(row[9]) if row[9] else None,
            partition_id=row[10],
            row_count_raw=row[11],
            row_count_curated=row[12],
            issue_count=row[13],
            error_code=row[14],
            error_message=row[15],
        )

    def _quality_issue_to_row(self, issue: QualityIssue) -> tuple[object, ...]:
        return (
            issue.issue_id,
            issue.import_run_id,
            issue.dataset_id,
            issue.symbol,
            issue.freq.value if issue.freq else None,
            issue.trading_date.isoformat() if issue.trading_date else None,
            issue.bar_start_time.isoformat() if issue.bar_start_time else None,
            issue.issue_code,
            issue.severity.value,
            issue.message,
            issue.raw_ref,
        )

    def _row_to_quality_issue(self, row) -> QualityIssue:
        return QualityIssue(
            issue_id=row[0],
            import_run_id=row[1],
            dataset_id=row[2],
            symbol=row[3],
            freq=Frequency(row[4]) if row[4] else None,
            trading_date=date.fromisoformat(row[5]) if row[5] else None,
            bar_start_time=datetime.fromisoformat(row[6]) if row[6] else None,
            issue_code=row[7],
            severity=Severity(row[8]),
            message=row[9],
            raw_ref=row[10],
        )
