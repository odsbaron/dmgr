from datetime import UTC, datetime, timedelta

import duckdb
import polars as pl

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import ComputeMode, FactorRunConfig, FactorSpec
from quant_research.factors.registry import RegisteredFactor
from quant_research.features.contracts import FeatureCommitRequest, FeatureRunStatus
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.quality import FactorQualityAnalyzer, QualityStatus


def factor_spec(
    factor_id: str,
    *,
    output_field: str | None = None,
    warmup_bars: int = 0,
    forward_bars: int = 0,
) -> FactorSpec:
    return FactorSpec(
        factor_id=factor_id,
        version="1.0.0",
        namespace="price",
        description=f"{factor_id} test factor.",
        input_fields=("close",),
        output_fields=(output_field or factor_id,),
        supported_freqs=(Frequency.D1,),
        lookback_bars=max(1, warmup_bars + 1),
        warmup_bars=warmup_bars,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
        quality_rules={
            "forward_bars": forward_bars,
            "causal": forward_bars == 0,
        },
    )


def factor_frame(*, include_as_of: bool = True, duplicate: bool = False) -> pl.LazyFrame:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    rows = []
    for index in range(2):
        row = {
            "dataset_id": "fixture-daily",
            "symbol": "000001.SZ",
            "freq": "1d",
            "ret_1": None if index == 0 else 0.01,
            "ma_3": None,
        }
        if include_as_of:
            row["as_of"] = start + timedelta(days=index)
        rows.append(row)
    if duplicate:
        rows.append(rows[-1].copy())
    return pl.DataFrame(rows).lazy()


def run_config(factor_run_id: str = "factor-run-1") -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id=factor_run_id,
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        factor_ids=("ret_1", "ma_3"),
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def request(
    *,
    frame: pl.LazyFrame | None = None,
    resolved_factors: tuple[RegisteredFactor, ...] | None = None,
    factor_run_id: str = "factor-run-1",
) -> FeatureCommitRequest:
    return FeatureCommitRequest(
        config=run_config(factor_run_id),
        factor_frame=frame if frame is not None else factor_frame(),
        resolved_factors=resolved_factors
        or (
            RegisteredFactor(factor_spec("ret_1", warmup_bars=1), compute=None),
            RegisteredFactor(factor_spec("ma_3", warmup_bars=2), compute=None),
        ),
        input_row_count=2,
    )


def test_feature_store_writes_manifest_table_and_snapshot(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")

    result = store.commit_run(request())

    assert result.status == FeatureRunStatus.COMMITTED
    assert result.snapshot_ref is not None
    assert result.snapshot_ref.table == "feature_snapshot"
    assert result.row_count_feature == 4
    assert result.row_count_snapshot == 2

    manifest = store.get_manifest("factor-run-1")
    feature_rows = store.read_feature_table(result.feature_table_ref)
    snapshots = store.read_snapshot(result.snapshot_ref)

    assert manifest is not None
    assert manifest.status == FeatureRunStatus.COMMITTED
    assert manifest.row_count_feature == 4
    assert manifest.row_count_snapshot == 2
    assert manifest.code_version == "0.1.0"
    assert manifest.config_hash.startswith("sha256:")
    assert {(row.factor_id, row.output_field) for row in feature_rows} == {
        ("ret_1", "ret_1"),
        ("ma_3", "ma_3"),
    }
    assert {row.trading_date for row in feature_rows} == {"2026-07-01", "2026-07-02"}
    assert snapshots[1].features == {"ret_1": 0.01, "ma_3": None}

    with duckdb.connect(str(tmp_path / "research.duckdb")) as conn:
        index_sql = conn.execute(
            """
            SELECT sql FROM duckdb_indexes()
            WHERE index_name = 'idx_feature_table_research_lookup'
            """
        ).fetchone()[0]
    assert all(
        column in index_sql
        for column in ("dataset_id", "feature_set_id", "freq", "trading_date", "symbol")
    )


def test_feature_store_rejects_missing_key_column_and_writes_failed_manifest(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")

    result = store.commit_run(request(frame=factor_frame(include_as_of=False)))

    assert result.status == FeatureRunStatus.FAILED
    assert result.error_code == "MISSING_KEY_COLUMN"
    assert result.snapshot_ref is None
    manifest = store.get_manifest("factor-run-1")
    assert manifest is not None
    assert manifest.status == FeatureRunStatus.FAILED
    assert manifest.error_code == "MISSING_KEY_COLUMN"
    assert store.read_feature_table(result.feature_table_ref) == []


def test_feature_store_adds_nullable_lineage_columns_to_existing_manifest_table(tmp_path):
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE factor_run_manifest (
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
                quality_status VARCHAR NOT NULL,
                quality_summary_json VARCHAR NOT NULL,
                error_code VARCHAR,
                error_message VARCHAR
            )
            """
        )

    LocalDuckDBFeatureStore(db_path)

    with duckdb.connect(str(db_path)) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info('factor_run_manifest')").fetchall()
        }
    assert {
        "universe_ref",
        "universe_id",
        "universe_version",
        "universe_definition_hash",
        "universe_snapshot_set_hash",
        "market_data_ref",
        "market_dataset_version",
        "market_data_definition_hash",
        "market_data_snapshot_set_hash",
    } <= columns


def test_feature_store_rejects_missing_declared_factor_output(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    frame = factor_frame().drop("ma_3")

    result = store.commit_run(request(frame=frame))

    assert result.status == FeatureRunStatus.FAILED
    assert result.error_code == "MISSING_FACTOR_OUTPUT"
    assert store.read_feature_table(result.feature_table_ref) == []


def test_feature_store_rejects_duplicate_feature_key(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")

    result = store.commit_run(request(frame=factor_frame(duplicate=True)))

    assert result.status == FeatureRunStatus.FAILED
    assert result.error_code == "DUPLICATE_FEATURE_KEY"
    assert store.read_feature_table(result.feature_table_ref) == []


def test_feature_store_rejects_duplicate_snapshot_output_field(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    duplicate_output_factors = (
        RegisteredFactor(factor_spec("ret_1", output_field="shared"), compute=None),
        RegisteredFactor(factor_spec("ma_3", output_field="shared"), compute=None),
    )

    result = store.commit_run(request(resolved_factors=duplicate_output_factors))

    assert result.status == FeatureRunStatus.FAILED
    assert result.error_code == "DUPLICATE_OUTPUT_FIELD"
    assert store.read_feature_table(result.feature_table_ref) == []


def test_feature_store_rejects_recommit_of_committed_run(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    first = store.commit_run(request())

    second = store.commit_run(request())

    assert first.status == FeatureRunStatus.COMMITTED
    assert second.status == FeatureRunStatus.FAILED
    assert second.error_code == "FEATURE_RUN_ALREADY_COMMITTED"
    assert store.get_manifest("factor-run-1").status == FeatureRunStatus.COMMITTED
    assert len(store.read_feature_table(first.feature_table_ref)) == 4


def test_feature_store_writes_quality_metrics_and_updates_manifest(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    commit = store.commit_run(request())
    values = store.read_feature_table(commit.feature_table_ref)
    report = FactorQualityAnalyzer().analyze(values, request().resolved_factors)

    store.commit_quality_report(report)

    metrics = store.list_quality_metrics("factor-run-1")
    manifest = store.get_manifest("factor-run-1")

    assert any(metric.metric_name == "null_ratio" for metric in metrics)
    assert any(metric.metric_name == "future_leakage_count" for metric in metrics)
    assert manifest.quality_status == QualityStatus.PASSED.value
    assert manifest.quality_summary["status"] == "PASSED"
    assert manifest.quality_report_ref == (
        "duckdb://factor_quality_metric?factor_run_id=factor-run-1"
    )


def test_feature_store_marks_manifest_quality_failed_for_forward_leakage(tmp_path):
    store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    forward_factor = (
        RegisteredFactor(
            factor_spec("ret_1", warmup_bars=0, forward_bars=1),
            compute=None,
        ),
        RegisteredFactor(
            factor_spec("ma_3", warmup_bars=0),
            compute=None,
        ),
    )
    commit = store.commit_run(request(resolved_factors=forward_factor))
    values = store.read_feature_table(commit.feature_table_ref)
    report = FactorQualityAnalyzer().analyze(values, forward_factor)

    store.commit_quality_report(report)

    leakage = [
        metric
        for metric in store.list_quality_metrics("factor-run-1")
        if metric.metric_name == "future_leakage_count" and metric.factor_id == "ret_1"
    ][0]
    manifest = store.get_manifest("factor-run-1")

    assert leakage.metric_value == 2
    assert leakage.severity.value == "ERROR"
    assert manifest.quality_status == QualityStatus.FAILED.value
