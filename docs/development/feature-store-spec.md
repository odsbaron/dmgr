# FeatureStore Development Spec

> Branch: `feature/factor-operator-dsl`
> Scope: factor result frame -> DuckDB feature assets -> snapshot `DataRef`.
> Status: implementation-ready spec for the next development slice.

Current implementation slice:

- `quant_research.features.contracts.FeatureCommitRequest`
- `quant_research.features.contracts.FeatureCommitResult`
- `quant_research.features.contracts.FeatureRunManifest`
- `quant_research.features.contracts.FeatureValue`
- `quant_research.features.contracts.FeatureSnapshot`
- `quant_research.features.transform.wide_to_feature_values`
- `quant_research.features.transform.build_feature_snapshots`
- `quant_research.features.duckdb_store.LocalDuckDBFeatureStore`
- `quant_research.features.quality.FactorQualityAnalyzer`
- `quant_research.features.gates.FeatureQualityGate`
- `factor_quality_metric` DuckDB table

Detailed quality-check guide:

- `docs/development/factor-quality-checks.md`

## 1. Purpose

`FeatureStore` turns factor computation results into durable research assets.

It sits after `PolarsFactorRunner`:

```text
DataRef(curated_market_bar)
  -> FactorRegistry
  -> PolarsFactorRunner
  -> factor result LazyFrame
  -> FeatureStore.commit_run(...)
  -> feature_table
  -> feature_snapshot
  -> factor_run_manifest
  -> DataRef(feature_snapshot)
```

The key idea: factor results are not useful enough if they only exist as an in-memory `LazyFrame`. Once committed through `FeatureStore`, they become reproducible, traceable, and consumable by later strategy, replay, and quasi-live modules.

## 2. Responsibilities

FeatureStore must do:

| Responsibility | Meaning |
|---|---|
| Validate factor result schema | Ensure key columns and declared factor output columns exist. |
| Convert wide factor frame to long feature rows | Write one row per `symbol + freq + as_of + factor + output_field`. |
| Build feature snapshots | Write one row per `symbol + freq + as_of` with `features_json`. |
| Write run manifest | Record input refs, factor versions, row counts, status, engine, and errors. |
| Return snapshot `DataRef` | Give downstream consumers a logical reference, not a table path or raw SQL. |
| Preserve atomicity | Avoid partial feature writes on failure. |

FeatureStore must not do:

| Non-responsibility | Owner |
|---|---|
| Read raw CSV / Parquet K lines | data ingestion layer |
| Normalize bars | data ingestion layer |
| Compute factor formulas | factor runner |
| Choose strategy signals | strategy layer |
| Run portfolio/risk/order logic | trading runtime |
| Repair bad factor values silently | quality layer |
| Decide if a snapshot can enter backtest/training/strategy consumption | consumer-side quality gate |

## 3. Boundary Contract

### 3.1 Input

`FeatureStore.commit_run(...)` consumes:

```python
FeatureCommitRequest(
    config=FactorRunConfig,
    factor_frame=pl.LazyFrame,
    resolved_factors=tuple[RegisteredFactor, ...],
    input_row_count=int | None,
)
```

Required `factor_frame` key columns:

```text
dataset_id
symbol
freq
as_of
```

Optional but recommended context columns:

```text
trading_date
exchange
asset_class
source_run_id
```

Factor value columns are derived from:

```text
resolved_factor.spec.output_fields
```

FeatureStore should not infer factor columns by scanning all non-key columns. Inference is too easy to get wrong once intermediate columns appear.

### 3.2 Output

Successful commit returns:

```python
FeatureCommitResult(
    factor_run_id="factor-run-1",
    status=FeatureRunStatus.COMMITTED,
    snapshot_ref=DataRef(...),
    feature_table_ref=DataRef(...),
    manifest_ref=DataRef(...),
    row_count_feature=...,
    row_count_snapshot=...,
)
```

Primary returned ref:

```text
duckdb://feature_snapshot?feature_set_id=<id>&factor_run_id=<run>&dataset_id=<dataset>&freq=<freq>
```

### 3.3 Audit Read vs Consumption Read

Feature assets can exist even when quality failed. Keep two read paths separate:

```python
# Audit/debug path: may read failed-quality assets.
feature_store.read_snapshot(snapshot_ref)

# Consumption path: only reads PASSED committed assets.
FeatureQualityGate(feature_store).read_consumable_snapshot(snapshot_ref)
```

Rules:

| Path | Intended use | Quality behavior |
|---|---|---|
| `LocalDuckDBFeatureStore.read_snapshot` | Audit, debugging, reports | Does not enforce consumption policy. |
| `FeatureQualityGate.read_consumable_snapshot` | Backtest, training, strategy | Requires `manifest.status=COMMITTED` and `quality_status=PASSED`. |

This preserves the distinction:

```text
feature_snapshot exists != feature_snapshot is consumable
```

## 4. Contracts

### 4.1 `FeatureRunStatus`

```text
CREATED
RUNNING
COMMITTED
FAILED
```

### 4.2 `FeatureCommitRequest`

Fields:

| Field | Type | Rule |
|---|---|---|
| `config` | `FactorRunConfig` | Source of ids, refs, freq, dataset, execution mode. |
| `factor_frame` | `pl.LazyFrame` | Output from `PolarsFactorRunner`. |
| `resolved_factors` | tuple[`RegisteredFactor`, ...] | Factor specs and versions used for commit. |
| `input_row_count` | int or null | Optional upstream bar count for manifest. |

### 4.3 `FeatureCommitResult`

Fields:

| Field | Type | Rule |
|---|---|---|
| `factor_run_id` | string | Same as `config.factor_run_id`. |
| `status` | `FeatureRunStatus` | `COMMITTED` or `FAILED`. |
| `snapshot_ref` | `DataRef` or null | Present only on success. |
| `feature_table_ref` | `DataRef` or null | Present only on success. |
| `manifest_ref` | `DataRef` | Always present if manifest was written. |
| `row_count_feature` | int | Long feature rows written. |
| `row_count_snapshot` | int | Snapshot rows written. |
| `error_code` | string or null | Present on failure. |
| `error_message` | string or null | Present on failure. |

### 4.4 `FeatureSnapshot`

Reader-facing contract:

| Field | Type | Meaning |
|---|---|---|
| `snapshot_id` | string | Stable id from `feature_set_id + dataset_id + symbol + freq + as_of + factor_run_id`. |
| `feature_set_id` | string | Logical feature set. |
| `dataset_id` | string | Input dataset. |
| `symbol` | string | Instrument. |
| `freq` | string | `1d`, `1m`, etc. |
| `as_of` | string | Factor value timestamp as ISO string. |
| `features` | dict | `{output_field: value}`. |
| `factor_run_ids` | list[string] | Usually one run id in MVP-1. |
| `input_data_refs` | list[string] | Source bar refs. |
| `warmup_complete` | bool | True only when all included values are warm. |
| `quality_flags` | list[string] | Row-level warnings. |
| `feature_ref` | string | Logical self-reference. |

## 5. DuckDB Tables

### 5.1 `factor_run_manifest`

One row per factor store commit.

| Column | Type | Notes |
|---|---|---|
| `factor_run_id` | text | Primary run id. |
| `feature_set_id` | text | Logical feature set. |
| `dataset_id` | text | Source dataset. |
| `freq` | text | Frequency. |
| `input_data_refs_json` | text | JSON array. |
| `factor_versions_json` | text | `{factor_id: version}`. |
| `factor_output_fields_json` | text | `{factor_id: [fields...]}`. |
| `engine` | text | `polars`. |
| `execution_mode` | text | `lazy`, `streaming`, or `eager_debug`. |
| `status` | text | `CREATED`, `RUNNING`, `COMMITTED`, `FAILED`. |
| `started_at` | text | ISO timestamp. |
| `finished_at` | text | ISO timestamp or null. |
| `row_count_input` | integer | Input rows if known. |
| `row_count_feature` | integer | Long rows written. |
| `row_count_snapshot` | integer | Snapshot rows written. |
| `quality_status` | text | `NOT_RUN`, `PASSED`, `WARNING`, or `FAILED`. |
| `quality_summary_json` | text | Compact summary from `FactorQualityReport.summary`. |
| `error_code` | text | Nullable. |
| `error_message` | text | Nullable. |

### 5.2 `feature_table`

Long format feature values.

| Column | Type | Notes |
|---|---|---|
| `factor_run_id` | text | Run lineage. |
| `feature_set_id` | text | Feature set. |
| `dataset_id` | text | Dataset. |
| `symbol` | text | Instrument. |
| `freq` | text | Frequency. |
| `as_of` | text | Factor value timestamp as ISO string. |
| `factor_id` | text | Factor id. |
| `factor_version` | text | Factor version. |
| `output_field` | text | Output field name. |
| `value_float` | double | Numeric value. |
| `value_string` | text | Non-numeric value. |
| `value_kind` | text | `float`, `string`, `bool`, or `null`. |
| `warmup_complete` | boolean | Factor-level warmup flag. |
| `quality_flags_json` | text | JSON array. |
| `input_data_ref` | text | Bar input ref. |
| `created_at` | text | ISO timestamp. |

Uniqueness key:

```text
feature_set_id, dataset_id, symbol, freq, as_of, factor_id, factor_version, output_field
```

### 5.3 `feature_snapshot`

Strategy/replay-friendly feature view.

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | text | Stable id. |
| `feature_set_id` | text | Feature set. |
| `dataset_id` | text | Dataset. |
| `symbol` | text | Instrument. |
| `freq` | text | Frequency. |
| `as_of` | text | Factor value timestamp as ISO string. |
| `features_json` | text | `{output_field: value}`. |
| `factor_run_ids_json` | text | JSON array. |
| `input_data_refs_json` | text | JSON array. |
| `warmup_complete` | boolean | All included values are warm. |
| `quality_flags_json` | text | JSON array. |
| `feature_ref` | text | Logical ref. |
| `created_at` | text | ISO timestamp. |

Uniqueness key:

```text
feature_set_id, dataset_id, symbol, freq, as_of, factor_run_ids_json
```

## 6. Wide-To-Long Conversion

`PolarsFactorRunner` returns a wide frame:

```text
symbol | freq | as_of | close | ret_1 | ma_3 | ...
```

FeatureStore converts only declared output fields into long rows:

```text
symbol | freq | as_of | factor_id | factor_version | output_field | value
```

Rules:

1. Key columns are copied from the factor frame.
2. Output fields come from `RegisteredFactor.spec.output_fields`.
3. Intermediate columns are ignored unless declared as output fields.
4. Null values are written with `value_kind = null`.
5. Numeric values prefer `value_float`.
6. Non-numeric values use `value_string`.

## 7. Snapshot Aggregation

`feature_snapshot` groups long rows by:

```text
feature_set_id, dataset_id, symbol, freq, as_of
```

It builds:

```json
{
  "ret_1": 0.01,
  "ma_3": 10.5
}
```

Rules:

1. `features_json` key is `output_field`.
2. If two factors produce the same `output_field`, commit must fail with `DUPLICATE_OUTPUT_FIELD`.
3. `warmup_complete` is true only if every included long row is warm.
4. `feature_ref` points back to the snapshot slice:

```text
duckdb://feature_snapshot?feature_set_id=<id>&factor_run_id=<run>&symbol=<symbol>&freq=<freq>&as_of=<timestamp>
```

## 8. Warmup Semantics

MVP-1 can derive warmup from factor metadata and per-symbol row order.

For each factor:

```text
warmup_complete = row_index_by_symbol >= factor.spec.warmup_bars
```

Where `row_index_by_symbol` is zero-based after sorting:

```text
symbol, as_of ascending
```

If a factor produces null after warmup, that is not automatically a warmup issue. It should later be handled by `factor_quality_metric`.

## 9. Commit Transaction

Recommended flow:

```text
commit_run(request)
  -> validate request
  -> create manifest RUNNING
  -> collect or stream factor frame according to config
  -> convert to feature_table rows
  -> validate duplicate feature keys
  -> build feature_snapshot rows
  -> BEGIN TRANSACTION
  -> delete previous rows for same factor_run_id if status is not COMMITTED
  -> insert feature_table rows
  -> insert feature_snapshot rows
  -> upsert manifest COMMITTED
  -> COMMIT
  -> return snapshot DataRef
```

Failure flow:

```text
on validation or write failure:
  -> ROLLBACK active transaction
  -> write or update manifest FAILED
  -> return FeatureCommitResult(status=FAILED)
```

Rule: committed feature rows and snapshots must never exist without a `COMMITTED` manifest.

## 10. Idempotency And Re-runs

MVP-1 rule:

```text
factor_run_id is immutable
```

If `factor_run_id` already has `COMMITTED` status, `FeatureStore` must reject a second commit with:

```text
FEATURE_RUN_ALREADY_COMMITTED
```

If `factor_run_id` exists as `FAILED`, a re-run may reuse the same id only if the caller explicitly sets `allow_failed_overwrite = true`. Default is false.

Config-hash based idempotency is deferred. It will be useful later, but the first version should keep the rule simple and auditable.

## 11. Validation Gate

FeatureStore must fail before writing feature rows if:

1. Required key columns are missing.
2. A declared output field is missing from the factor frame.
3. The factor frame has zero rows.
4. Duplicate feature keys exist in the long table.
5. Duplicate `output_field` values would collide in one snapshot.
6. A factor has unsupported value type.
7. A committed `factor_run_id` already exists.

Suggested error codes:

```text
MISSING_KEY_COLUMN
MISSING_FACTOR_OUTPUT
EMPTY_FACTOR_FRAME
DUPLICATE_FEATURE_KEY
DUPLICATE_OUTPUT_FIELD
UNSUPPORTED_VALUE_TYPE
FEATURE_RUN_ALREADY_COMMITTED
FEATURE_STORE_WRITE_FAILED
```

## 12. Minimal API Draft

```python
class FeatureStore(Protocol):
    def commit_run(self, request: FeatureCommitRequest) -> FeatureCommitResult:
        ...

    def get_manifest(self, factor_run_id: str) -> FeatureRunManifest | None:
        ...

    def read_snapshot(self, ref: DataRef) -> list[FeatureSnapshot]:
        ...

    def read_feature_table(self, ref: DataRef) -> list[FeatureValue]:
        ...
```

DuckDB implementation:

```python
class LocalDuckDBFeatureStore:
    def __init__(self, db_path: str | Path):
        ...
```

The implementation may live beside `LocalDuckDBStore` or in a new module:

```text
src/quant_research/features/
```

Preferred package split:

```text
features/contracts.py
features/duckdb_store.py
features/transform.py
```

## 13. Tests

Minimum TDD coverage:

| Test | Assertion |
|---|---|
| `test_feature_store_writes_manifest_table_and_snapshot` | One commit writes all three tables and returns snapshot ref. |
| `test_feature_store_converts_wide_factor_frame_to_long_rows` | `ret_1` and `ma_3` produce long rows with correct factor ids. |
| `test_feature_snapshot_groups_features_by_symbol_and_as_of` | Snapshot JSON contains both factor outputs. |
| `test_feature_store_rejects_missing_key_column` | Missing `as_of` fails before write. |
| `test_feature_store_rejects_missing_declared_output` | Missing factor output fails before write. |
| `test_feature_store_rejects_duplicate_feature_key` | Duplicate long key fails. |
| `test_feature_store_rejects_duplicate_snapshot_output_field` | Two factors producing same output field fail. |
| `test_feature_store_rejects_recommit_of_committed_run` | Immutable run id rule holds. |
| `test_failed_commit_writes_failed_manifest_without_feature_rows` | Failure leaves auditable manifest only. |

## 14. Implementation Order

Recommended next slice:

```text
feature/feature-store
```

Steps:

1. Add `features/contracts.py`.
2. Write failing tests for commit result and DuckDB tables.
3. Add DuckDB schema creation.
4. Implement wide-to-long conversion.
5. Implement snapshot aggregation.
6. Implement transactional `commit_run`.
7. Add failed manifest handling.
8. Add reader methods for snapshot and long feature rows.

## 15. Future Extensions

Deferred:

1. Config hash idempotency.
2. Partitioned Parquet export.
3. Feature matrix materialization for model training.
4. Cross-run snapshot merge.
5. Stream-compatible incremental feature cache.
6. Row-level `input_window_start` / `input_window_end` lineage.

## 16. Factor Quality Metrics

Detailed implementation guide:

```text
docs/development/factor-quality-checks.md
docs/development/factor-leakage-prefix-invariance-spec.md
```

Current implementation writes `factor_quality_metric` through:

```python
FactorQualityAnalyzer().analyze(feature_values, resolved_factors)
LocalDuckDBFeatureStore.commit_quality_report(report)
```

Metrics currently emitted per factor output:

```text
row_count
null_ratio
warmup_incomplete_count
duplicate_key_count
future_leakage_count
```

Manifest quality fields:

```text
quality_status
quality_summary_json
```

### 16.1 Future Leakage With Forward Calculation

The first implementation treats forward calculation as an explicit quality rule:

```python
quality_rules={
    "forward_bars": 1,
    "causal": False,
}
```

Rule:

```text
forward_bars > 0 OR causal = false OR uses_future_data = true
  -> future_leakage_count = row_count for that factor output
future_leakage_count > 0 -> severity = ERROR
any ERROR -> quality_status = FAILED
```

This is intentionally conservative. A forward-return or label-style factor should not pass as a normal feature. Later, row-level lineage can strengthen the check:

```text
input_window_end > as_of -> future_leakage_count += 1
```
