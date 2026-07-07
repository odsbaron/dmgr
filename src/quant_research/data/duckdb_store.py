from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

import duckdb

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun, ImportStatus
from quant_research.contracts.quality import QualityIssue, QualityReport, Severity
from quant_research.contracts.refs import DataRef


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

    def read_bars(self, data_ref: DataRef | str) -> list[BarRecord]:
        ref = DataRef.parse(data_ref) if isinstance(data_ref, str) else data_ref
        if ref.table != "curated_market_bar":
            raise ValueError(f"unsupported DuckDB table for BarRecord reads: {ref.table}")

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
                    raw_ref VARCHAR
                )
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
