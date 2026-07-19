from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from itertools import count

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportRun
from quant_research.contracts.quality import QualityReport
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.partition_contracts import (
    MarketDataImportRun,
    MarketDataPartition,
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)
from quant_research.contracts.source import SourceType
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry
from quant_research.features.contracts import FeatureRunStatus
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.leakage import CutpointSelectionMode, PrefixProbeConfig
from quant_research.features.quality import FactorQualityAnalyzer, QualitySeverity, QualityStatus
from quant_research.pipeline.contracts import ResearchRunRequest, ResearchRunStatus
from quant_research.pipeline.research import ResearchPipeline
from quant_research.universe.contracts import (
    UniverseDefinition,
    UniverseSourceSpec,
    UniverseSourceType,
)
from quant_research.universe.duckdb_store import LocalDuckDBUniverseStore
from quant_research.universe.ingestion import UniverseIngestionService
from quant_research.universe.resolver import UniverseResolver


def bar(close: str, index: int, *, symbol: str = "000001.SZ") -> BarRecord:
    start = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=start,
        bar_end_time=start,
        open="10.0",
        high="20.0",
        low="1.0",
        close=close,
        volume="1000",
        turnover="10000",
        adjustment=Adjustment.NONE,
        source="csv",
        source_run_id="import-run-1",
        source_row_id=f"row-{index}",
        raw_ref="fixture.csv",
    )


def import_run() -> ImportRun:
    return ImportRun.create(
        import_run_id="import-run-1",
        dataset_id="fixture-daily",
        source_id="fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="sha256:fixture",
    )


def seed_bars(store: LocalDuckDBStore):
    return store.commit_import(
        import_run(),
        [
            bar("10.0", 0),
            bar("11.0", 1),
            bar("12.0", 2),
        ],
        QualityReport("import-run-1", ()),
    )


def registry_with_ret_1(*, max_null_ratio: float = 1.0) -> FactorRegistry:
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="ret_1",
        version="1.0.0",
        namespace="price",
        description="One bar historical return.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=2,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
        quality_rules={"max_null_ratio": max_null_ratio},
    )
    registry.register(spec, op.pct_change(field("close"), periods=1).alias("ret_1"))
    return registry


def pipeline(
    db_path,
    registry: FactorRegistry,
    *,
    universe_resolver: UniverseResolver | None = None,
) -> ResearchPipeline:
    return ResearchPipeline(
        data_store=LocalDuckDBStore(db_path),
        factor_registry=registry,
        factor_runner=PolarsFactorRunner(registry),
        feature_store=LocalDuckDBFeatureStore(db_path),
        quality_analyzer=FactorQualityAnalyzer(),
        universe_resolver=universe_resolver,
    )


def request(input_data_ref: str, **overrides) -> ResearchRunRequest:
    params = {
        "factor_run_id": "factor-run-1",
        "feature_set_id": "basic_price_v1",
        "input_data_ref": input_data_ref,
        "factor_ids": ("ret_1",),
    }
    params.update(overrides)
    return ResearchRunRequest(**params)


def seed_multi_symbol_bars(store: LocalDuckDBStore):
    bars = [
        bar("10.0", 0, symbol="A"),
        bar("20.0", 1, symbol="A"),
        bar("10.0", 0, symbol="B"),
        bar("11.0", 1, symbol="B"),
        bar("10.0", 0, symbol="C"),
        bar("12.0", 1, symbol="C"),
    ]
    return store.commit_import(import_run(), bars, QualityReport("import-run-1", ()))


def universe_definition() -> UniverseDefinition:
    return UniverseDefinition(
        universe_id="ashare-research",
        version="v1",
        name="A-share research universe",
        asset_class=AssetClass.EQUITY,
        calendar_id="XSHG_XSHE",
        timezone="Asia/Shanghai",
        selection_cutoff_time=time(9, 30),
    )


def seed_universe(db_path, tmp_path, dates_and_members=None):
    universe_store = LocalDuckDBUniverseStore(db_path)
    sequence = count(1)
    ingestion = UniverseIngestionService(
        universe_store,
        run_id_factory=lambda: f"universe-run-{next(sequence)}",
    )
    dates_and_members = dates_and_members or (
        (date(2026, 7, 1), ("A", "B")),
        (date(2026, 7, 2), ("B", "C")),
    )
    for trading_date, members in dates_and_members:
        path = tmp_path / f"universe-{trading_date.isoformat()}.csv"
        rows = "".join(f"{member},{trading_date.isoformat()}\n" for member in members)
        path.write_text(f"symbol,trading_date\n{rows}", encoding="utf-8")
        ingestion.ingest(
            universe_definition(),
            UniverseSourceSpec(
                source_id=f"universe-{trading_date.isoformat()}",
                universe_id="ashare-research",
                universe_version="v1",
                source_type=UniverseSourceType.CSV,
                path=str(path),
                trading_date=trading_date,
                known_at=datetime.combine(trading_date, time(1), tzinfo=UTC),
                source_data_cutoff=datetime.combine(
                    date.fromordinal(trading_date.toordinal() - 1),
                    time(7),
                    tzinfo=UTC,
                ),
                field_mapping={
                    "instrument_id": "symbol",
                    "trading_date": "trading_date",
                },
            ),
        )
    snapshot_set = universe_store.create_snapshot_set(
        universe_id="ashare-research",
        universe_version="v1",
        trading_dates=tuple(value[0] for value in dates_and_members),
    )
    return UniverseResolver(universe_store), snapshot_set.ref


def market_definition(*, asset_class: AssetClass = AssetClass.EQUITY):
    return MarketDatasetDefinition(
        dataset_id="fixture-daily",
        version="v1",
        name="Fixture exact daily bars",
        asset_class=asset_class,
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        calendar_id="XSHG_XSHE",
        timezone="Asia/Shanghai",
    )


def seed_exact_market_data(
    store: LocalDuckDBStore,
    *,
    asset_class: AssetClass = AssetClass.EQUITY,
):
    definition = market_definition(asset_class=asset_class)
    store.register_market_dataset_definition(definition)
    dates = (date(2026, 7, 1), date(2026, 7, 2))
    symbols = ("A", "B", "C")
    for index, trading_date in enumerate(dates):
        source_run_id = f"market-run-{index + 1}"
        spec = MarketDataSourceSpec(
            source_id=f"market-source-{index + 1}",
            dataset_id="fixture-daily",
            dataset_version="v1",
            source_type=SourceType.CSV,
            path=f"fixture-{trading_date.isoformat()}.csv",
            trading_date=trading_date,
            known_at=datetime.combine(trading_date, time(8), tzinfo=UTC),
            source_data_cutoff=datetime.combine(trading_date, time(7), tzinfo=UTC),
            field_mapping={
                "symbol": "symbol",
                "exchange": "exchange",
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            },
        )
        partition_bars = tuple(
            replace(
                bar(
                    str(10 + index + symbol_index),
                    index,
                    symbol=symbol,
                ),
                exchange="CFFEX" if asset_class == AssetClass.FUTURE else "XSHE",
                asset_class=asset_class,
                source_run_id=source_run_id,
            )
            for symbol_index, symbol in enumerate(symbols)
        )
        run = MarketDataImportRun.create(
            import_run_id=source_run_id,
            spec=spec,
            source_file_hash=f"sha256:file-{index + 1}",
            definition_hash=definition.definition_hash,
        )
        partition = MarketDataPartition.create(
            definition,
            spec,
            partition_bars,
            source_file_hash=f"sha256:file-{index + 1}",
        )
        store.commit_market_data_partition(
            replace(run, row_count_raw=len(partition_bars)),
            partition,
            QualityReport(source_run_id, ()),
        )
    snapshot_set = store.create_market_data_snapshot_set(
        dataset_id="fixture-daily",
        dataset_version="v1",
        trading_dates=dates,
    )
    return snapshot_set.ref, snapshot_set


def test_research_pipeline_commits_quality_passed_feature_run(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(request(data_ref.uri, symbols=("000001.SZ",)))

    assert result.status == ResearchRunStatus.COMMITTED
    assert result.quality_status == QualityStatus.PASSED
    assert result.consumable is True
    assert result.block_reason is None
    assert result.feature_table_ref is not None
    assert result.snapshot_ref is not None
    assert result.manifest_ref is not None
    assert result.row_count_input == 3
    assert result.row_count_feature == 3
    assert result.row_count_snapshot == 3
    assert result.metric_count > 0

    manifest = service.feature_store.get_manifest("factor-run-1")
    snapshots = service.feature_store.read_snapshot(result.snapshot_ref)

    assert manifest.quality_status == QualityStatus.PASSED.value
    assert snapshots[-1].features["ret_1"] == pytest.approx(12.0 / 11.0 - 1.0)


def test_research_pipeline_keeps_failed_quality_assets_but_blocks_consumption(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1(max_null_ratio=0.0))

    result = service.run(request(data_ref.uri))

    metrics = service.feature_store.list_quality_metrics("factor-run-1")
    null_ratio = [metric for metric in metrics if metric.metric_name == "null_ratio"][0]

    assert result.status == ResearchRunStatus.QUALITY_FAILED
    assert result.quality_status == QualityStatus.FAILED
    assert result.consumable is False
    assert result.block_reason == "quality_failed"
    assert result.snapshot_ref is not None
    assert null_ratio.severity == QualitySeverity.ERROR


def test_research_pipeline_blocks_when_prefix_probe_cannot_run_requested_cutpoint(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(
        request(
            data_ref.uri,
            prefix_probe_config=PrefixProbeConfig(
                cutpoint_mode=CutpointSelectionMode.EXPLICIT,
                explicit_cutpoints=("2026-07-10T07:00:00+00:00",),
                min_prefix_rows=1,
            ),
        )
    )

    warning_count = [
        metric
        for metric in service.feature_store.list_quality_metrics("factor-run-1")
        if metric.metric_name == "prefix_probe_warning_count"
    ][0]

    assert result.status == ResearchRunStatus.QUALITY_FAILED
    assert result.quality_status == QualityStatus.FAILED
    assert result.consumable is False
    assert result.block_reason == "quality_failed"
    assert warning_count.metric_value == 1
    assert warning_count.severity == QualitySeverity.ERROR


def test_research_pipeline_reports_invalid_input_ref_as_pipeline_failure(tmp_path):
    service = pipeline(tmp_path / "research.duckdb", registry_with_ret_1())

    result = service.run(request("duckdb://feature_table?dataset_id=fixture-daily&freq=1d"))

    assert result.status == ResearchRunStatus.FAILED
    assert result.quality_status == QualityStatus.NOT_RUN
    assert result.consumable is False
    assert result.block_reason == "pipeline_failed"
    assert result.error_step == "parse_input_ref"
    assert result.error_code == "INVALID_INPUT_DATA_REF"


def test_research_pipeline_persists_failed_manifest_after_reading_input(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(request(data_ref.uri, factor_ids=("unknown_factor",)))

    manifest = service.feature_store.get_manifest("factor-run-1")
    assert result.status == ResearchRunStatus.FAILED
    assert result.error_step == "resolve_factors"
    assert result.manifest_ref is not None
    assert manifest is not None
    assert manifest.status == FeatureRunStatus.FAILED
    assert manifest.error_code == "RESOLVE_FACTORS_FAILED"
    assert manifest.input_data_refs == (data_ref.uri,)


def test_research_pipeline_applies_daily_universe_after_preserving_lookback(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_multi_symbol_bars(LocalDuckDBStore(db_path))
    universe_resolver, universe_ref = seed_universe(db_path, tmp_path)
    service = pipeline(
        db_path,
        registry_with_ret_1(),
        universe_resolver=universe_resolver,
    )

    result = service.run(
        request(
            data_ref.uri,
            universe_ref=universe_ref.uri,
            as_of_start=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
            as_of_end=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
        )
    )

    assert result.status == ResearchRunStatus.COMMITTED
    assert result.snapshot_ref is not None
    snapshots = service.feature_store.read_snapshot(result.snapshot_ref)
    assert [snapshot.symbol for snapshot in snapshots] == ["B", "C"]
    assert snapshots[0].features["ret_1"] == pytest.approx(0.1)
    assert snapshots[1].features["ret_1"] == pytest.approx(0.2)
    manifest = service.feature_store.get_manifest("factor-run-1")
    assert manifest is not None
    assert manifest.universe_ref == universe_ref.uri
    assert manifest.universe_id == "ashare-research"
    assert manifest.universe_version == "v1"
    assert manifest.universe_definition_hash == universe_definition().definition_hash
    assert manifest.universe_snapshot_set_hash is not None


def test_research_pipeline_intersects_universe_with_debug_symbols(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_multi_symbol_bars(LocalDuckDBStore(db_path))
    universe_resolver, universe_ref = seed_universe(db_path, tmp_path)
    service = pipeline(
        db_path,
        registry_with_ret_1(),
        universe_resolver=universe_resolver,
    )

    result = service.run(
        request(
            data_ref.uri,
            universe_ref=universe_ref.uri,
            symbols=("B",),
            as_of_start=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
        )
    )

    assert result.snapshot_ref is not None
    snapshots = service.feature_store.read_snapshot(result.snapshot_ref)
    assert [snapshot.symbol for snapshot in snapshots] == ["B"]
    assert snapshots[0].features["ret_1"] == pytest.approx(0.1)


def test_research_pipeline_reports_unresolved_universe_as_pipeline_failure(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    universe_store = LocalDuckDBUniverseStore(db_path)
    service = pipeline(
        db_path,
        registry_with_ret_1(),
        universe_resolver=UniverseResolver(universe_store),
    )

    result = service.run(
        request(
            data_ref.uri,
            universe_ref="duckdb://universe_member?snapshot_set_id=unknown",
        )
    )

    assert result.status == ResearchRunStatus.FAILED
    assert result.error_step == "resolve_universe"
    assert result.error_code == "UNKNOWN_SNAPSHOT_SET"


def test_legacy_run_keeps_pre_start_history_for_factor_lookback(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_ref = seed_bars(LocalDuckDBStore(db_path))
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(
        request(
            data_ref.uri,
            as_of_start=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
            as_of_end=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
        )
    )

    assert result.snapshot_ref is not None
    snapshots = service.feature_store.read_snapshot(result.snapshot_ref)
    assert len(snapshots) == 1
    assert snapshots[0].features["ret_1"] == pytest.approx(0.1)
    manifest = service.feature_store.get_manifest("factor-run-1")
    assert manifest is not None
    assert manifest.universe_ref is None
    assert manifest.market_data_ref is None
    assert manifest.market_dataset_version is None
    assert manifest.market_data_definition_hash is None
    assert manifest.market_data_snapshot_set_hash is None


def test_exact_market_data_run_commits_snapshot_set_lineage(tmp_path):
    db_path = tmp_path / "research.duckdb"
    data_store = LocalDuckDBStore(db_path)
    market_data_ref, snapshot_set = seed_exact_market_data(data_store)
    service = pipeline(db_path, registry_with_ret_1())

    result = service.run(request(market_data_ref.uri))

    assert result.status == ResearchRunStatus.COMMITTED
    assert result.row_count_input == 6
    manifest = service.feature_store.get_manifest("factor-run-1")
    assert manifest is not None
    assert manifest.market_data_ref == market_data_ref.uri
    assert manifest.market_dataset_version == "v1"
    assert manifest.market_data_definition_hash == market_definition().definition_hash
    assert manifest.market_data_snapshot_set_hash == snapshot_set.snapshot_set_hash


def test_exact_market_data_dates_block_incomplete_universe_even_without_member_bars(
    tmp_path,
):
    db_path = tmp_path / "research.duckdb"
    market_data_ref, _ = seed_exact_market_data(LocalDuckDBStore(db_path))
    universe_resolver, universe_ref = seed_universe(
        db_path,
        tmp_path,
        dates_and_members=((date(2026, 7, 1), ("A",)),),
    )
    service = pipeline(
        db_path,
        registry_with_ret_1(),
        universe_resolver=universe_resolver,
    )

    result = service.run(request(market_data_ref.uri, universe_ref=universe_ref.uri))

    assert result.status == ResearchRunStatus.FAILED
    assert result.error_step == "validate_universe"
    assert result.error_code == "UNIVERSE_DATE_NOT_COVERED"
    assert "2026-07-02" in result.error_message


def test_exact_market_data_definition_asset_class_blocks_mismatched_universe(tmp_path):
    db_path = tmp_path / "research.duckdb"
    market_data_ref, _ = seed_exact_market_data(
        LocalDuckDBStore(db_path),
        asset_class=AssetClass.FUTURE,
    )
    universe_resolver, universe_ref = seed_universe(db_path, tmp_path)
    service = pipeline(
        db_path,
        registry_with_ret_1(),
        universe_resolver=universe_resolver,
    )

    result = service.run(request(market_data_ref.uri, universe_ref=universe_ref.uri))

    assert result.status == ResearchRunStatus.FAILED
    assert result.error_code == "UNIVERSE_ASSET_CLASS_MISMATCH"


def test_exact_market_data_unknown_snapshot_set_is_structured_pipeline_failure(tmp_path):
    service = pipeline(tmp_path / "research.duckdb", registry_with_ret_1())

    result = service.run(request("duckdb://curated_market_bar?snapshot_set_id=unknown"))

    assert result.status == ResearchRunStatus.FAILED
    assert result.error_step == "resolve_market_data"
    assert result.error_code == "UNKNOWN_SNAPSHOT_SET"


def test_exact_market_data_ref_rejects_mixed_legacy_filters(tmp_path):
    service = pipeline(tmp_path / "research.duckdb", registry_with_ret_1())

    result = service.run(
        request("duckdb://curated_market_bar?snapshot_set_id=set-1&dataset_id=fixture-daily")
    )

    assert result.status == ResearchRunStatus.FAILED
    assert result.error_step == "resolve_market_data"
    assert result.error_code == "INVALID_MARKET_DATA_REF"
