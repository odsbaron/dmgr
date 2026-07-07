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

Implemented entry points:

- `quant_research.data.ingestion.DataIngestionService`
- `quant_research.data.duckdb_store.LocalDuckDBStore`
- `quant_research.data.readers.csv_reader.CSVKLineReader`

Next implementation lane:

- Polars-backed factor layer: `DataRef -> FactorSpec / FactorRegistry -> Polars runner -> feature_table / feature_snapshot / factor_run_manifest`

Verification:

```bash
.venv/bin/python -m pytest -v
.venv/bin/ruff check src tests
```
