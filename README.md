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

Next implementation lane:

- FeatureStore: `PolarsFactorRunner output -> feature_table / feature_snapshot / factor_run_manifest -> DataRef(feature_snapshot)`

Supported factor authoring modes:

- Production: `Operator DSL-lite -> OperatorRegistry -> PolarsFactorRunner`
- Research: native Polars `polars_expr` or `frame_transform`

Verification:

```bash
.venv/bin/python -m pytest -v
.venv/bin/ruff check src tests
```
