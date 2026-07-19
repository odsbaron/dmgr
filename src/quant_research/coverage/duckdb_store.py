from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from quant_research.contracts.bar import Frequency
from quant_research.contracts.refs import DataRef
from quant_research.coverage.contracts import (
    CoverageIssue,
    CoverageIssueSeverity,
    CoverageMetric,
    CoveragePolicy,
    CoverageReportRef,
    CoverageRunManifest,
    CoverageRunResult,
    CoverageRunStatus,
    CoverageScope,
    TimestampConvention,
    coverage_issue_ref,
    coverage_metric_ref,
)


class CoverageStoreError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class LocalDuckDBCoverageStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit(
        self,
        manifest: CoverageRunManifest,
        metrics: tuple[CoverageMetric, ...],
        issues: tuple[CoverageIssue, ...],
    ) -> CoverageRunResult:
        existing = self.get_manifest(manifest.coverage_run_id)
        if existing is not None:
            if existing.config_hash != manifest.config_hash:
                raise CoverageStoreError(
                    "COVERAGE_RUN_CONFLICT",
                    "coverage_run_id already exists with a different config hash",
                )
            return self._result(existing, reused_existing=True)

        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    INSERT INTO coverage_run_manifest VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    self._manifest_row(manifest),
                )
                if metrics:
                    conn.executemany(
                        """
                        INSERT INTO coverage_metric VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        [self._metric_row(metric) for metric in metrics],
                    )
                if issues:
                    conn.executemany(
                        """
                        INSERT INTO coverage_issue VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        [self._issue_row(issue) for issue in issues],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self._result(manifest, reused_existing=False)

    def get_manifest(
        self,
        ref: CoverageReportRef | DataRef | str,
    ) -> CoverageRunManifest | None:
        if isinstance(ref, str) and not ref.startswith("duckdb://"):
            run_id = ref
        else:
            run_id = CoverageReportRef.parse(ref).coverage_run_id
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT coverage_run_id, config_hash, status, policy,
                       timestamp_convention, freq, date_start, date_end,
                       minimum_coverage_ratio, market_data_ref, calendar_ref,
                       universe_ref, daily_status_ref, market_data_hash,
                       calendar_hash, universe_hash, daily_status_hash,
                       expected_bar_count, actual_bar_count, matched_bar_count,
                       missing_bar_count, unexpected_bar_count, coverage_ratio,
                       issue_count, consumable, started_at, finished_at,
                       code_version, error_code, error_message
                FROM coverage_run_manifest
                WHERE coverage_run_id = ?
                """,
                [run_id],
            ).fetchone()
        return None if row is None else self._row_to_manifest(row)

    def read_metrics(self, ref: DataRef | str) -> tuple[CoverageMetric, ...]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        self._validate_child_ref(data_ref, "coverage_metric")
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT coverage_run_id, scope, trading_date, symbol,
                       expected_bar_count, actual_bar_count, matched_bar_count,
                       missing_bar_count, unexpected_bar_count, coverage_ratio
                FROM coverage_metric
                WHERE coverage_run_id = ?
                ORDER BY CASE scope
                    WHEN 'SYMBOL_DATE' THEN 0
                    WHEN 'DATE' THEN 1
                    ELSE 2
                END, trading_date, symbol
                """,
                [data_ref.filters["coverage_run_id"]],
            ).fetchall()
        return tuple(
            CoverageMetric(
                coverage_run_id=row[0],
                scope=CoverageScope(row[1]),
                trading_date=date.fromisoformat(row[2]) if row[2] else None,
                symbol=row[3],
                expected_bar_count=row[4],
                actual_bar_count=row[5],
                matched_bar_count=row[6],
                missing_bar_count=row[7],
                unexpected_bar_count=row[8],
                coverage_ratio=row[9],
            )
            for row in rows
        )

    def read_issues(self, ref: DataRef | str) -> tuple[CoverageIssue, ...]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        self._validate_child_ref(data_ref, "coverage_issue")
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT coverage_run_id, issue_code, severity, message,
                       trading_date, symbol, expected_at, actual_at
                FROM coverage_issue
                WHERE coverage_run_id = ?
                ORDER BY issue_id
                """,
                [data_ref.filters["coverage_run_id"]],
            ).fetchall()
        return tuple(
            CoverageIssue(
                coverage_run_id=row[0],
                issue_code=row[1],
                severity=CoverageIssueSeverity(row[2]),
                message=row[3],
                trading_date=date.fromisoformat(row[4]) if row[4] else None,
                symbol=row[5],
                expected_at=datetime.fromisoformat(row[6]) if row[6] else None,
                actual_at=datetime.fromisoformat(row[7]) if row[7] else None,
            )
            for row in rows
        )

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS coverage_run_manifest (
                    coverage_run_id VARCHAR PRIMARY KEY,
                    config_hash VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    policy VARCHAR NOT NULL,
                    timestamp_convention VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    date_start VARCHAR NOT NULL,
                    date_end VARCHAR NOT NULL,
                    minimum_coverage_ratio DOUBLE NOT NULL,
                    market_data_ref VARCHAR NOT NULL,
                    calendar_ref VARCHAR NOT NULL,
                    universe_ref VARCHAR NOT NULL,
                    daily_status_ref VARCHAR NOT NULL,
                    market_data_hash VARCHAR,
                    calendar_hash VARCHAR,
                    universe_hash VARCHAR,
                    daily_status_hash VARCHAR,
                    expected_bar_count BIGINT NOT NULL,
                    actual_bar_count BIGINT NOT NULL,
                    matched_bar_count BIGINT NOT NULL,
                    missing_bar_count BIGINT NOT NULL,
                    unexpected_bar_count BIGINT NOT NULL,
                    coverage_ratio DOUBLE NOT NULL,
                    issue_count BIGINT NOT NULL,
                    consumable BOOLEAN NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR NOT NULL,
                    code_version VARCHAR NOT NULL,
                    error_code VARCHAR,
                    error_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS coverage_metric (
                    coverage_run_id VARCHAR NOT NULL,
                    scope VARCHAR NOT NULL,
                    trading_date VARCHAR,
                    symbol VARCHAR,
                    expected_bar_count BIGINT NOT NULL,
                    actual_bar_count BIGINT NOT NULL,
                    matched_bar_count BIGINT NOT NULL,
                    missing_bar_count BIGINT NOT NULL,
                    unexpected_bar_count BIGINT NOT NULL,
                    coverage_ratio DOUBLE NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS coverage_issue (
                    issue_id VARCHAR PRIMARY KEY,
                    coverage_run_id VARCHAR NOT NULL,
                    issue_code VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    message VARCHAR NOT NULL,
                    trading_date VARCHAR,
                    symbol VARCHAR,
                    expected_at VARCHAR,
                    actual_at VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS coverage_metric_run_idx
                ON coverage_metric (coverage_run_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS coverage_issue_run_idx
                ON coverage_issue (coverage_run_id)
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    @staticmethod
    def _manifest_row(manifest: CoverageRunManifest) -> list[object]:
        return [
            manifest.coverage_run_id,
            manifest.config_hash,
            manifest.status.value,
            manifest.policy.value,
            manifest.timestamp_convention.value,
            manifest.freq.value,
            manifest.date_start.isoformat(),
            manifest.date_end.isoformat(),
            manifest.minimum_coverage_ratio,
            manifest.market_data_ref,
            manifest.calendar_ref,
            manifest.universe_ref,
            manifest.daily_status_ref,
            manifest.market_data_hash,
            manifest.calendar_hash,
            manifest.universe_hash,
            manifest.daily_status_hash,
            manifest.expected_bar_count,
            manifest.actual_bar_count,
            manifest.matched_bar_count,
            manifest.missing_bar_count,
            manifest.unexpected_bar_count,
            manifest.coverage_ratio,
            manifest.issue_count,
            manifest.consumable,
            manifest.started_at.isoformat(),
            manifest.finished_at.isoformat(),
            manifest.code_version,
            manifest.error_code,
            manifest.error_message,
        ]

    @staticmethod
    def _metric_row(metric: CoverageMetric) -> list[object]:
        return [
            metric.coverage_run_id,
            metric.scope.value,
            metric.trading_date.isoformat() if metric.trading_date else None,
            metric.symbol,
            metric.expected_bar_count,
            metric.actual_bar_count,
            metric.matched_bar_count,
            metric.missing_bar_count,
            metric.unexpected_bar_count,
            metric.coverage_ratio,
        ]

    @staticmethod
    def _issue_row(issue: CoverageIssue) -> list[object]:
        return [
            issue.issue_id,
            issue.coverage_run_id,
            issue.issue_code,
            issue.severity.value,
            issue.message,
            issue.trading_date.isoformat() if issue.trading_date else None,
            issue.symbol,
            issue.expected_at.isoformat() if issue.expected_at else None,
            issue.actual_at.isoformat() if issue.actual_at else None,
        ]

    @staticmethod
    def _row_to_manifest(row) -> CoverageRunManifest:
        return CoverageRunManifest(
            coverage_run_id=row[0],
            config_hash=row[1],
            status=CoverageRunStatus(row[2]),
            policy=CoveragePolicy(row[3]),
            timestamp_convention=TimestampConvention(row[4]),
            freq=Frequency(row[5]),
            date_start=date.fromisoformat(row[6]),
            date_end=date.fromisoformat(row[7]),
            minimum_coverage_ratio=row[8],
            market_data_ref=row[9],
            calendar_ref=row[10],
            universe_ref=row[11],
            daily_status_ref=row[12],
            market_data_hash=row[13],
            calendar_hash=row[14],
            universe_hash=row[15],
            daily_status_hash=row[16],
            expected_bar_count=row[17],
            actual_bar_count=row[18],
            matched_bar_count=row[19],
            missing_bar_count=row[20],
            unexpected_bar_count=row[21],
            coverage_ratio=row[22],
            issue_count=row[23],
            consumable=row[24],
            started_at=datetime.fromisoformat(row[25]),
            finished_at=datetime.fromisoformat(row[26]),
            code_version=row[27],
            error_code=row[28],
            error_message=row[29],
        )

    @staticmethod
    def _validate_child_ref(ref: DataRef, table: str) -> None:
        if ref.table != table:
            raise ValueError(f"coverage ref must point to {table}")
        if set(ref.filters) != {"coverage_run_id"} or not ref.filters["coverage_run_id"]:
            raise ValueError("coverage ref requires only coverage_run_id")

    @staticmethod
    def _result(
        manifest: CoverageRunManifest,
        *,
        reused_existing: bool,
    ) -> CoverageRunResult:
        run_id = manifest.coverage_run_id
        return CoverageRunResult(
            coverage_run_id=run_id,
            status=manifest.status,
            consumable=manifest.consumable,
            expected_bar_count=manifest.expected_bar_count,
            actual_bar_count=manifest.actual_bar_count,
            matched_bar_count=manifest.matched_bar_count,
            missing_bar_count=manifest.missing_bar_count,
            unexpected_bar_count=manifest.unexpected_bar_count,
            coverage_ratio=manifest.coverage_ratio,
            issue_count=manifest.issue_count,
            manifest_ref=DataRef.parse(CoverageReportRef(run_id).uri),
            metric_ref=coverage_metric_ref(run_id),
            issue_ref=coverage_issue_ref(run_id),
            reused_existing=reused_existing,
            error_code=manifest.error_code,
            error_message=manifest.error_message,
        )
