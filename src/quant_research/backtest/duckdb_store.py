from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from quant_research.backtest.contracts import (
    BacktestConflictError,
    BacktestMetric,
    BacktestRunManifest,
    BacktestRunResult,
    BacktestRunStatus,
    Fill,
    NavSnapshot,
    PositionSnapshot,
    Side,
)
from quant_research.contracts.refs import DataRef


class LocalDuckDBBacktestStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_run(
        self,
        manifest: BacktestRunManifest,
        *,
        fills: list[Fill],
        positions: list[PositionSnapshot],
        nav_snapshots: list[NavSnapshot],
        metrics: list[BacktestMetric],
    ) -> BacktestRunResult:
        existing = self.get_manifest(manifest.backtest_run_id)
        if existing is not None:
            if existing.config_hash != manifest.config_hash:
                raise BacktestConflictError(
                    "BACKTEST_RUN_CONFLICT",
                    "backtest_run_id already exists with a different config hash",
                )
            return self._result(existing, reused_existing=True)

        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    INSERT INTO backtest_run_manifest (
                        backtest_run_id, target_source_ref, market_data_ref, initial_cash,
                        execution_config_json, cost_config_json, status, started_at, finished_at,
                        config_hash, content_hash, code_version, row_count_fill,
                        row_count_position, row_count_nav, row_count_metric, universe_ref,
                        calendar_ref, daily_status_ref, coverage_report_ref
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    self._manifest_to_row(manifest),
                )
                if fills:
                    conn.executemany(
                        """
                        INSERT INTO backtest_fill (
                            fill_id, backtest_run_id, rebalance_as_of, execution_time,
                            trading_date, symbol, side, quantity, price, notional, cost
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [self._fill_to_row(fill) for fill in fills],
                    )
                if positions:
                    conn.executemany(
                        """
                        INSERT INTO backtest_position (
                            backtest_run_id, trading_date, as_of, symbol, quantity,
                            close_price, market_value, portfolio_weight
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [self._position_to_row(position) for position in positions],
                    )
                if nav_snapshots:
                    conn.executemany(
                        """
                        INSERT INTO backtest_nav (
                            backtest_run_id, trading_date, as_of, cash, market_value, nav
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [self._nav_to_row(snapshot) for snapshot in nav_snapshots],
                    )
                if metrics:
                    conn.executemany(
                        """
                        INSERT INTO backtest_metric (
                            backtest_run_id, metric_name, metric_value, metric_json
                        ) VALUES (?, ?, ?, ?)
                        """,
                        [
                            (
                                metric.backtest_run_id,
                                metric.metric_name,
                                metric.metric_value,
                                json.dumps(metric.metric_json, sort_keys=True),
                            )
                            for metric in metrics
                        ],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self._result(manifest, reused_existing=False)

    def get_manifest(self, backtest_run_id: str) -> BacktestRunManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT backtest_run_id, target_source_ref, market_data_ref, initial_cash,
                       execution_config_json, cost_config_json, status, started_at, finished_at,
                       config_hash, content_hash, code_version, row_count_fill,
                       row_count_position, row_count_nav, row_count_metric, universe_ref,
                       calendar_ref, daily_status_ref, coverage_report_ref
                FROM backtest_run_manifest
                WHERE backtest_run_id = ?
                """,
                [backtest_run_id],
            ).fetchone()
        return self._row_to_manifest(row) if row else None

    def read_fills(self, backtest_run_id: str) -> list[Fill]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT fill_id, backtest_run_id, rebalance_as_of, execution_time,
                       trading_date, symbol, side, quantity, price, notional, cost
                FROM backtest_fill
                WHERE backtest_run_id = ?
                ORDER BY execution_time, fill_id
                """,
                [backtest_run_id],
            ).fetchall()
        return [self._row_to_fill(row) for row in rows]

    def read_nav(self, backtest_run_id: str) -> list[NavSnapshot]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT backtest_run_id, trading_date, as_of, cash, market_value, nav
                FROM backtest_nav
                WHERE backtest_run_id = ?
                ORDER BY trading_date
                """,
                [backtest_run_id],
            ).fetchall()
        return [
            NavSnapshot(
                backtest_run_id=row[0],
                trading_date=row[1],
                as_of=datetime.fromisoformat(row[2]),
                cash=Decimal(row[3]),
                market_value=Decimal(row[4]),
                nav=Decimal(row[5]),
            )
            for row in rows
        ]

    def read_metrics(self, backtest_run_id: str) -> list[BacktestMetric]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT backtest_run_id, metric_name, metric_value, metric_json
                FROM backtest_metric
                WHERE backtest_run_id = ?
                ORDER BY metric_name
                """,
                [backtest_run_id],
            ).fetchall()
        return [
            BacktestMetric(
                backtest_run_id=row[0],
                metric_name=row[1],
                metric_value=row[2],
                metric_json=json.loads(row[3]),
            )
            for row in rows
        ]

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_run_manifest (
                    backtest_run_id VARCHAR PRIMARY KEY,
                    target_source_ref VARCHAR NOT NULL,
                    market_data_ref VARCHAR NOT NULL,
                    initial_cash VARCHAR NOT NULL,
                    execution_config_json VARCHAR NOT NULL,
                    cost_config_json VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at VARCHAR NOT NULL,
                    finished_at VARCHAR NOT NULL,
                    config_hash VARCHAR NOT NULL,
                    content_hash VARCHAR,
                    code_version VARCHAR NOT NULL,
                    row_count_fill BIGINT NOT NULL,
                    row_count_position BIGINT NOT NULL,
                    row_count_nav BIGINT NOT NULL,
                    row_count_metric BIGINT NOT NULL,
                    universe_ref VARCHAR,
                    calendar_ref VARCHAR,
                    daily_status_ref VARCHAR,
                    coverage_report_ref VARCHAR
                )
                """
            )
            conn.execute(
                "ALTER TABLE backtest_run_manifest ADD COLUMN IF NOT EXISTS content_hash VARCHAR"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_fill (
                    fill_id VARCHAR PRIMARY KEY,
                    backtest_run_id VARCHAR NOT NULL,
                    rebalance_as_of VARCHAR NOT NULL,
                    execution_time VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    symbol VARCHAR NOT NULL,
                    side VARCHAR NOT NULL,
                    quantity BIGINT NOT NULL,
                    price VARCHAR NOT NULL,
                    notional VARCHAR NOT NULL,
                    cost VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_position (
                    backtest_run_id VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    as_of VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    quantity BIGINT NOT NULL,
                    close_price VARCHAR NOT NULL,
                    market_value VARCHAR NOT NULL,
                    portfolio_weight DOUBLE NOT NULL,
                    PRIMARY KEY (backtest_run_id, trading_date, symbol)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_nav (
                    backtest_run_id VARCHAR NOT NULL,
                    trading_date DATE NOT NULL,
                    as_of VARCHAR NOT NULL,
                    cash VARCHAR NOT NULL,
                    market_value VARCHAR NOT NULL,
                    nav VARCHAR NOT NULL,
                    PRIMARY KEY (backtest_run_id, trading_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_metric (
                    backtest_run_id VARCHAR NOT NULL,
                    metric_name VARCHAR NOT NULL,
                    metric_value DOUBLE NOT NULL,
                    metric_json VARCHAR NOT NULL,
                    PRIMARY KEY (backtest_run_id, metric_name)
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _result(self, manifest: BacktestRunManifest, *, reused_existing: bool) -> BacktestRunResult:
        run_id = manifest.backtest_run_id
        return BacktestRunResult(
            manifest=manifest,
            manifest_ref=DataRef("backtest_run_manifest", {"backtest_run_id": run_id}),
            fill_ref=DataRef("backtest_fill", {"backtest_run_id": run_id}),
            position_ref=DataRef("backtest_position", {"backtest_run_id": run_id}),
            nav_ref=DataRef("backtest_nav", {"backtest_run_id": run_id}),
            metric_ref=DataRef("backtest_metric", {"backtest_run_id": run_id}),
            reused_existing=reused_existing,
        )

    def _manifest_to_row(self, manifest: BacktestRunManifest) -> tuple[object, ...]:
        return (
            manifest.backtest_run_id,
            manifest.target_source_ref,
            manifest.market_data_ref,
            str(manifest.initial_cash),
            json.dumps(manifest.execution_config, sort_keys=True),
            json.dumps(manifest.cost_config, sort_keys=True),
            manifest.status.value,
            manifest.started_at,
            manifest.finished_at,
            manifest.config_hash,
            manifest.content_hash,
            manifest.code_version,
            manifest.row_count_fill,
            manifest.row_count_position,
            manifest.row_count_nav,
            manifest.row_count_metric,
            manifest.universe_ref,
            manifest.calendar_ref,
            manifest.daily_status_ref,
            manifest.coverage_report_ref,
        )

    def _row_to_manifest(self, row) -> BacktestRunManifest:
        return BacktestRunManifest(
            backtest_run_id=row[0],
            target_source_ref=row[1],
            market_data_ref=row[2],
            initial_cash=Decimal(row[3]),
            execution_config=json.loads(row[4]),
            cost_config=json.loads(row[5]),
            status=BacktestRunStatus(row[6]),
            started_at=row[7],
            finished_at=row[8],
            config_hash=row[9],
            content_hash=row[10],
            code_version=row[11],
            row_count_fill=row[12],
            row_count_position=row[13],
            row_count_nav=row[14],
            row_count_metric=row[15],
            universe_ref=row[16],
            calendar_ref=row[17],
            daily_status_ref=row[18],
            coverage_report_ref=row[19],
        )

    def _fill_to_row(self, fill: Fill) -> tuple[object, ...]:
        return (
            fill.fill_id,
            fill.backtest_run_id,
            fill.rebalance_as_of.isoformat(),
            fill.execution_time.isoformat(),
            fill.trading_date,
            fill.symbol,
            fill.side.value,
            fill.quantity,
            str(fill.price),
            str(fill.notional),
            str(fill.cost),
        )

    def _row_to_fill(self, row) -> Fill:
        return Fill(
            fill_id=row[0],
            backtest_run_id=row[1],
            rebalance_as_of=datetime.fromisoformat(row[2]),
            execution_time=datetime.fromisoformat(row[3]),
            trading_date=row[4],
            symbol=row[5],
            side=Side(row[6]),
            quantity=row[7],
            price=Decimal(row[8]),
            notional=Decimal(row[9]),
            cost=Decimal(row[10]),
        )

    def _position_to_row(self, position: PositionSnapshot) -> tuple[object, ...]:
        return (
            position.backtest_run_id,
            position.trading_date,
            position.as_of.isoformat(),
            position.symbol,
            position.quantity,
            str(position.close_price),
            str(position.market_value),
            position.portfolio_weight,
        )

    def _nav_to_row(self, snapshot: NavSnapshot) -> tuple[object, ...]:
        return (
            snapshot.backtest_run_id,
            snapshot.trading_date,
            snapshot.as_of.isoformat(),
            str(snapshot.cash),
            str(snapshot.market_value),
            str(snapshot.nav),
        )
