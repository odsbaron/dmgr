# Quant Research MVP-0

K line batch research framework for local DuckDB-based data ingestion, validation,
factor computation, and reproducible research manifests.

Planning sources:

- `../openspec/changes/kline-batch-research-framework/`
- `../docs/superpowers/specs/2026-07-07-kline-batch-research-framework-design.md`
- `../docs/superpowers/plans/2026-07-07-kline-batch-research-framework-implementation.md`

The first implementation lane is data ingestion. It covers external CSV/Parquet
K line inputs, source registration, import runs, normalization, validation, and
DuckDB-backed `data_ref` emission.

