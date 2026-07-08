# Quant Research MVP-0

K line batch research framework for local DuckDB-based data ingestion, validation,
factor computation, and reproducible research manifests.

Planning sources in this workspace:

- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/specs/2026-07-07-kline-batch-research-framework-design.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/plans/2026-07-07-kline-batch-research-framework-implementation.md`

The first implementation lane is data ingestion. It currently covers CSV K line
inputs, source registration, import runs, normalization, validation,
DuckDB-backed `data_ref` emission, and idempotent replay by source-file hash.
Parquet is reserved behind the reader interface.

Development docs:

- `docs/development/data-ingestion-development.md`
- `docs/development/factor-layer-polars-spec.md`
- `docs/development/feature-store-spec.md`
- `docs/development/factor-quality-checks.md`
- `docs/development/factor-leakage-prefix-invariance-spec.md`
- `docs/development/research-pipeline-development.md`
- `docs/superpowers/plans/2026-07-08-research-pipeline-after-factor-quality.md`

Implemented entry points:

- `quant_research.data.ingestion.DataIngestionService`
- `quant_research.data.duckdb_store.LocalDuckDBStore`
- `quant_research.data.readers.csv_reader.CSVKLineReader`
- `quant_research.factors.contracts.FactorSpec`
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
- `quant_research.pipeline.contracts.ResearchRunRequest`
- `quant_research.pipeline.contracts.ResearchRunResult`
- `quant_research.pipeline.bar_frame.bars_to_factor_frame`
- `quant_research.pipeline.research.ResearchPipeline`

Next implementation lane:

- Consumer-side quality gate and CLI wrapper for the research pipeline

Supported factor authoring modes:

- Production: `Operator DSL-lite -> OperatorRegistry -> PolarsFactorRunner`
- Research: native Polars `polars_expr` or `frame_transform`

Verification:

```bash
.venv/bin/python -m pytest -v
.venv/bin/ruff check src tests
```
