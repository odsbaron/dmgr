# Quant Research MVP-0

K line batch research framework for local DuckDB-based data ingestion, validation,
factor computation, and reproducible research manifests.

Planning sources in this workspace:

- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/specs/2026-07-07-kline-batch-research-framework-design.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/plans/2026-07-07-kline-batch-research-framework-implementation.md`

The first implementation lane is data ingestion. It currently covers CSV and
Parquet K line inputs, source registration, import runs, normalization, validation,
DuckDB-backed `data_ref` emission, and idempotent replay by source-file hash.

Development docs:

- `docs/development/data-ingestion-development.md`
- `docs/development/factor-layer-polars-spec.md`
- `docs/development/feature-store-spec.md`
- `docs/development/factor-quality-checks.md`
- `docs/development/factor-leakage-prefix-invariance-spec.md`
- `docs/development/research-pipeline-development.md`
- `docs/development/immutable-market-data-partitions.md`
- `docs/development/universe-management.md`
- `docs/development/market-calendar-daily-status.md`
- `docs/development/label-training-dataset-lineage.md`
- `docs/superpowers/plans/2026-07-08-research-pipeline-after-factor-quality.md`

Implemented entry points:

- `quant_research.data.ingestion.DataIngestionService`
- `quant_research.data.duckdb_store.LocalDuckDBStore`
- `quant_research.data.readers.csv_reader.CSVKLineReader`
- `quant_research.data.readers.parquet_reader.ParquetKLineReader`
- `quant_research.factors.contracts.FactorSpec`
- `quant_research.factors.contracts.FactorContext`
- `quant_research.factors.builtin.default_factor_registry`
- `quant_research.factors.dsl.field`
- `quant_research.factors.dsl.op`
- `quant_research.factors.operators.OperatorRegistry`
- `quant_research.factors.registry.FactorRegistry`
- `quant_research.factors.polars.PolarsFactorRunner`
- `quant_research.features.contracts.FeatureCommitRequest`
- `quant_research.features.transform.wide_to_feature_values`
- `quant_research.features.transform.build_feature_snapshots`
- `quant_research.features.duckdb_store.LocalDuckDBFeatureStore`
- `quant_research.features.quality.FactorQualityAnalyzer`
- `quant_research.features.leakage.PrefixInvarianceLeakageDetector`
- `quant_research.features.leakage.prefix_report_to_quality_metrics`
- `quant_research.features.gates.FeatureQualityGate`
- `quant_research.labels.contracts.LabelValue`
- `quant_research.labels.generation.forward_return_labels_from_bars`
- `quant_research.labels.generation.feature_values_to_label_request`
- `quant_research.labels.duckdb_store.LocalDuckDBLabelStore`
- `quant_research.labels.quality.LabelQualityAnalyzer`
- `quant_research.labels.gates.LabelQualityGate`
- `quant_research.labels.pipeline.LabelPipeline`
- `quant_research.datasets.feature_matrix.TrainingFeatureMatrixBuilder`
- `quant_research.datasets.feature_matrix.labels_to_label_matrix`
- `quant_research.datasets.feature_matrix.snapshots_to_feature_matrix`
- `quant_research.pipeline.contracts.ResearchRunRequest`
- `quant_research.pipeline.contracts.ResearchRunResult`
- `quant_research.pipeline.bar_frame.bars_to_factor_frame`
- `quant_research.pipeline.research.ResearchPipeline`
- `quant_research.cli.app`

Next implementation lane:

- Expected-slot coverage from Calendar, Universe, and DailyStatus assets
- Unified feature/label orchestration above the independent research and label pipelines

Supported factor authoring modes:

- Production: `Operator DSL-lite -> OperatorRegistry -> PolarsFactorRunner`
- Research: native Polars `polars_expr` or `frame_transform`

Built-in factors:

- Returns: `ret_1`, `ret_5`, `log_ret_1`
- Moving averages: `ma_5`, `ma_20`, `close_over_ma20`
- Volatility: `true_range`, `volatility_20`

## Local Quickstart

Install the package and development tools:

```bash
.venv/bin/python -m pip install -e '.[dev]'
```

The default local database is `data/research.duckdb`. The commands also accept
`--db <path>` so tests and independent experiments can use isolated databases.

Ingest and validate daily bars, with an optional Parquet export:

```bash
.venv/bin/quant-research ingest-bars \
  --input tests/fixtures/bars_daily.csv \
  --freq 1d \
  --dataset demo-daily \
  --db data/research.duckdb \
  --export data/exports/parquet/demo-daily.parquet

.venv/bin/quant-research validate-bars \
  --dataset demo-daily \
  --freq 1d \
  --db data/research.duckdb
```

Compute built-in factors from curated bars:

```bash
.venv/bin/quant-research compute-factors \
  --dataset demo-daily \
  --feature-set basic-v1 \
  --freq 1d \
  --factors ret_1,ma_5 \
  --factor-run-id demo-factor-run \
  --db data/research.duckdb
```

Run the complete dependency-ordered pipeline with a YAML file:

```yaml
database: data/research.duckdb
input: tests/fixtures/bars_1m.csv
dataset: demo-minute
freq: 1m
source: local-minute-fixture
timezone: Asia/Shanghai
calendar: cn_stock_simple
feature_set: basic-v1
factors: [ret_1]
factor_run_id: demo-minute-run
export: data/exports/parquet/demo-minute.parquet
```

```bash
.venv/bin/quant-research run-pipeline --config pipeline.yml
```

`run-pipeline` executes file read, normalization, K-line validation, curated-bar
write, factor computation, feature write, feature quality validation, and manifest
write. It exits non-zero when either quality gate blocks consumption.

## DuckDB Assets

The main logical tables in `data/research.duckdb` are:

| Area | Tables |
|---|---|
| K-line ingestion | `import_run`, `curated_market_bar`, `bar_quality_issue` |
| Immutable market data | `market_data_definition`, `market_data_import_run`, `market_data_partition`, `market_data_snapshot_set_manifest`, `market_data_snapshot_set_item` |
| Factors and features | `factor_run_manifest`, `feature_table`, `feature_snapshot`, `factor_quality_metric` |
| Labels | `label_run_manifest`, `label_table`, `label_quality_metric` |
| Training datasets | `training_dataset_manifest` |

Pipeline boundaries use stable `duckdb://...` refs rather than exposing SQL
connections. Parquet is an input and optional export format; it does not replace
the internal refs. Generated database and export files live below `data/` and are
ignored by Git.

Verification:

```bash
.venv/bin/python -m pytest -v
.venv/bin/ruff check src tests
```
