from datetime import UTC, date, datetime

import duckdb

from quant_research.contracts.bar import Adjustment, AssetClass, Frequency
from quant_research.contracts.source import SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.ingestion import ImmutableMarketDataIngestionService
from quant_research.data.partition_contracts import MarketDataSourceSpec, MarketDatasetDefinition
from quant_research.features.quality import QualityStatus
from quant_research.labels.contracts import LabelSourceKind
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.pipeline import LabelPipeline, LabelRunRequest, LabelRunStatus
from quant_research.labels.quality import LabelQualityAnalyzer


def _definition() -> MarketDatasetDefinition:
    return MarketDatasetDefinition(
        dataset_id="ashare-1m",
        version="v1",
        name="A-share one-minute bars",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.M1,
        adjustment=Adjustment.NONE,
        calendar_id="xshg-xshe",
        timezone="Asia/Shanghai",
    )


def _exact_market_ref(tmp_path, store: LocalDuckDBStore) -> str:
    source_path = tmp_path / "bars.csv"
    source_path.write_text(
        "\n".join(
            [
                "symbol,exchange,datetime,open,high,low,close,volume",
                "000001.SZ,SZSE,2026-07-07T09:30:00+08:00,10,10,10,10,100",
                "000001.SZ,SZSE,2026-07-07T09:31:00+08:00,11,11,11,11,100",
            ]
        ),
        encoding="utf-8",
    )
    spec = MarketDataSourceSpec(
        source_id="source-1",
        dataset_id="ashare-1m",
        dataset_version="v1",
        source_type=SourceType.CSV,
        path=str(source_path),
        trading_date=date(2026, 7, 7),
        known_at=datetime(2026, 7, 7, 2, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 7, 1, 32, tzinfo=UTC),
        field_mapping={
            "symbol": "symbol",
            "exchange": "exchange",
            "datetime": "datetime",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
    )
    result = ImmutableMarketDataIngestionService(
        store,
        run_id_factory=lambda: "market-run-1",
    ).ingest(_definition(), spec)
    assert result.partition_id is not None
    snapshot_set = store.create_market_data_snapshot_set(
        dataset_id="ashare-1m",
        dataset_version="v1",
        trading_dates=(date(2026, 7, 7),),
    )
    return snapshot_set.ref.uri


def _request(source_ref: str) -> LabelRunRequest:
    return LabelRunRequest(
        label_run_id="label-run-exact",
        label_set_id="forward-return-v1",
        source_ref=source_ref,
        label_id="forward_ret_1",
        label_version="1.0.0",
        forward_bars=1,
    )


def test_exact_market_data_label_run_persists_resolved_lineage(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_store = LocalDuckDBStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)
    source_ref = _exact_market_ref(tmp_path, data_store)

    result = LabelPipeline(
        data_store=data_store,
        label_store=label_store,
        quality_analyzer=LabelQualityAnalyzer(max_null_ratio=0.6),
    ).run(_request(source_ref))
    manifest = label_store.get_manifest("label-run-exact")

    assert result.status == LabelRunStatus.COMMITTED
    assert result.quality_status == QualityStatus.PASSED
    assert manifest is not None
    assert manifest.source_kind == LabelSourceKind.MARKET_DATA
    assert manifest.source_ref == source_ref
    assert manifest.dataset_id == "ashare-1m"
    assert manifest.freq == "1m"
    assert manifest.forward_bars == 1
    assert manifest.market_dataset_version == "v1"
    assert manifest.market_data_definition_hash == _definition().definition_hash
    assert manifest.market_data_snapshot_set_hash is not None


def test_unknown_exact_market_data_ref_fails_before_commit(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_store = LocalDuckDBStore(db_path)
    label_store = LocalDuckDBLabelStore(db_path)

    result = LabelPipeline(
        data_store=data_store,
        label_store=label_store,
        quality_analyzer=LabelQualityAnalyzer(),
    ).run(
        _request("duckdb://curated_market_bar?snapshot_set_id=missing")
    )

    assert result.status == LabelRunStatus.FAILED
    assert result.error_step == "resolve_market_data"
    assert result.error_code == "UNKNOWN_SNAPSHOT_SET"
    assert label_store.get_manifest("label-run-exact") is None


def test_original_label_schema_receives_additive_lineage_columns(tmp_path):
    db_path = tmp_path / "legacy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE label_table (
                label_run_id VARCHAR, label_set_id VARCHAR, dataset_id VARCHAR,
                symbol VARCHAR, freq VARCHAR, as_of VARCHAR, label_id VARCHAR,
                label_version VARCHAR, value_float DOUBLE, value_string VARCHAR,
                value_kind VARCHAR, forward_bars BIGINT,
                source_factor_run_id VARCHAR, created_at VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE label_run_manifest (
                label_run_id VARCHAR PRIMARY KEY, label_set_id VARCHAR,
                source_factor_run_id VARCHAR, row_count_label BIGINT,
                status VARCHAR, created_at VARCHAR, quality_status VARCHAR,
                quality_summary_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO label_run_manifest VALUES
            ('legacy-run', 'legacy-set', 'factor-run-old', 0, 'COMMITTED',
             '2026-07-07T00:00:00+00:00', 'PASSED', '{}')
            """
        )

    store = LocalDuckDBLabelStore(db_path)
    manifest = store.get_manifest("legacy-run")

    assert manifest is not None
    assert manifest.source_kind == LabelSourceKind.LEGACY
    assert manifest.source_ref == "factor-run-old"
    with duckdb.connect(str(db_path)) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('label_run_manifest')").fetchall()
        }
    assert "market_data_definition_hash" in columns
    assert "universe_snapshot_set_hash" in columns
