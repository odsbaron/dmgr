# Quant Research MVP-0

K line batch research framework for local DuckDB-based data ingestion, validation,
factor computation, and reproducible research manifests.

Planning sources in this workspace:

- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/specs/2026-07-07-kline-batch-research-framework-design.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/plans/2026-07-07-kline-batch-research-framework-implementation.md`

The first implementation lane is data ingestion. It covers external CSV/Parquet
K line inputs, source registration, import runs, normalization, validation, and
DuckDB-backed `data_ref` emission.

Development docs:

- `docs/development/data-ingestion-development.md`
