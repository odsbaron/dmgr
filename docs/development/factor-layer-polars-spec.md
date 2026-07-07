# Polars Factor Layer Development Spec

> Branch: `feature/data-ingestion-spec`
> Scope: `DataRef` -> Polars batch factor compute -> DuckDB feature tables -> `FeatureSnapshot`-compatible output.
> Status: implementation-ready spec for the next development lane.

## 1. Purpose

The factor layer turns validated K line data into reusable research features. It must start from the existing `duckdb://curated_market_bar?...` `DataRef`, not from raw CSV paths.

This layer is still research-side only. It does not run strategies, create orders, manage accounts, connect gateways, or make simulated fills.

Primary output:

```text
duckdb://feature_snapshot?feature_set_id=...&freq=...&factor_run_id=...
```

Secondary outputs:

```text
duckdb://feature_table?feature_set_id=...&factor_id=...
duckdb://factor_run_manifest?factor_run_id=...
duckdb://factor_quality_metric?factor_run_id=...
```

## 2. Technology Position

Polars is the default batch compute engine because factor computation is naturally columnar and expression-oriented. The implementation should prefer `pl.LazyFrame`, vectorized `pl.Expr`, and deferred collection.

Important boundary rule:

```text
Domain contracts: FactorSpec, DataRef, FeatureSnapshot, manifests
Adapter internals: Polars LazyFrame, Polars Expr, DuckDB connection details
```

External consumers should not need to import Polars to understand what a factor is or to consume a feature snapshot.

Technical basis:

- Polars lazy execution enables query optimization, streaming, and schema checking before processing.
- Polars can read database query results into DataFrames through `read_database` / `read_database_uri`.
- Lazy streaming can be requested with `collect(engine="streaming")`, but some operations may fall back to the in-memory engine.
- Time-series grouping and rolling logic require correctly sorted date/time columns.

References:

- https://docs.pola.rs/user-guide/lazy/using/
- https://docs.pola.rs/user-guide/concepts/streaming/
- https://docs.pola.rs/user-guide/io/database/
- https://docs.pola.rs/user-guide/transformations/time-series/rolling/

## 3. Goals / Non-Goals

Goals:

- Define factor contracts that are independent of physical storage.
- Register factor metadata and compute functions through `FactorRegistry`.
- Compute daily and minute factors from `curated_market_bar`.
- Persist factor outputs into DuckDB tables.
- Produce `FeatureSnapshot`-compatible rows for later strategy, replay, and quasi-live layers.
- Generate quality metrics for every factor run.
- Keep factor computation deterministic and auditable.

Non-goals:

- No live streaming engine in this lane.
- No real-time incremental factor cache yet.
- No cross-sectional neutralization against industry/market-cap data until reference datasets exist.
- No trading signal, portfolio construction, order routing, or risk checks.
- No distributed Polars Cloud or Spark execution in MVP-1.

## 4. Architecture

```text
DataRef(curated_market_bar)
  -> FeatureInputStore.resolve_bars(data_ref)
  -> BarFrame(normalized LazyFrame)
  -> FactorRegistry.resolve(factor_ids)
  -> PolarsFactorPlanner
  -> PolarsFactorRunner
  -> FactorQualityAnalyzer
  -> FeatureStore.commit_run(...)
  -> feature_table
  -> feature_snapshot
  -> factor_run_manifest
  -> factor_quality_metric
```

Component responsibilities:

| Component | Responsibility | Must not do |
|---|---|---|
| `FactorSpec` | Describe a factor's inputs, outputs, parameters, windows, supported frequencies, and engine. | Hold executable code. |
| `FactorRegistry` | Map `factor_id + version` to spec and compute adapter. | Query DuckDB directly. |
| `FeatureInputStore` | Resolve `DataRef` into a canonical factor input frame. | Compute factors. |
| `PolarsFactorPlanner` | Validate specs, input fields, frequency, windows, and build lazy expression plans. | Write feature tables. |
| `PolarsFactorRunner` | Execute the Polars plan and return a typed result frame. | Decide strategy semantics. |
| `FactorQualityAnalyzer` | Calculate nulls, coverage, warmup loss, duplicates, and leakage checks. | Repair factor values silently. |
| `FeatureStore` | Persist outputs, snapshots, manifests, and quality metrics. | Know individual factor formulas. |

## 5. Core Contracts

### 5.1 `FactorSpec`

Required fields:

| Field | Type | Rule |
|---|---|---|
| `factor_id` | string | Stable id, such as `ret_1`, `ma_20`, `volatility_20`. |
| `version` | string | Semantic or date version, such as `1.0.0`. |
| `namespace` | string | Example: `price`, `volume`, `microstructure`. |
| `description` | string | Human-readable purpose. |
| `input_fields` | tuple[str, ...] | Canonical bar columns required by the factor. |
| `output_fields` | tuple[str, ...] | Columns produced by the factor. |
| `params_schema` | dict | Parameter names, types, defaults, and bounds. |
| `supported_freqs` | tuple[Frequency, ...] | At minimum `1d` and/or `1m`. |
| `lookback_bars` | int | Number of historical bars required at `as_of`. |
| `warmup_bars` | int | Rows that may produce null values before the window is ready. |
| `compute_engine` | string | `polars` for this lane. |
| `compute_mode` | string | `expr`, `frame`, or `python_udf`. |
| `output_dtype` | dict[str, str] | Example: `{"ma_20": "float64"}`. |
| `quality_rules` | dict | Null thresholds, extreme bounds, and coverage expectations. |
| `tags` | tuple[str, ...] | Search/discovery tags. |

Example:

```python
FactorSpec(
    factor_id="ret_1",
    version="1.0.0",
    namespace="price",
    description="One-bar close-to-close return.",
    input_fields=("close",),
    output_fields=("ret_1",),
    params_schema={},
    supported_freqs=(Frequency.D1, Frequency.M1),
    lookback_bars=2,
    warmup_bars=1,
    compute_engine="polars",
    compute_mode="expr",
    output_dtype={"ret_1": "float64"},
    quality_rules={"max_null_ratio": 0.05},
    tags=("return", "momentum"),
)
```

### 5.2 Compute Function Types

MVP-1 supports two first-class compute styles and one escape hatch:

```python
PolarsExprFactory = Callable[[FactorSpec, FactorRunConfig], list[pl.Expr]]
PolarsFrameTransform = Callable[[pl.LazyFrame, FactorSpec, FactorRunConfig], pl.LazyFrame]
PythonUdfFactor = Callable[[pl.DataFrame, FactorSpec, FactorRunConfig], pl.DataFrame]
```

Rules:

1. Prefer `PolarsExprFactory` for simple rolling, return, moving average, volatility, and volume factors.
2. Use `PolarsFrameTransform` when the factor needs multiple intermediate columns or cross-column logic.
3. Use `PythonUdfFactor` only when vectorized Polars expressions are not practical. The manifest must mark `compute_mode = python_udf`.

### 5.3 `FactorRunConfig`

Required fields:

| Field | Type | Rule |
|---|---|---|
| `factor_run_id` | string | Unique run id. |
| `feature_set_id` | string | Logical feature set, such as `basic_price_v1`. |
| `input_data_ref` | DataRef | Must point to curated bars. |
| `factor_ids` | tuple[str, ...] | Requested factors. |
| `freq` | Frequency | Must match input bars. |
| `dataset_id` | string | Input dataset. |
| `as_of_start` | datetime or null | Optional lower bound. |
| `as_of_end` | datetime or null | Optional upper bound. |
| `symbols` | tuple[str, ...] or null | Optional symbol filter. |
| `engine` | string | `polars`. |
| `execution_mode` | string | `lazy`, `streaming`, or `eager_debug`. |
| `strict_quality` | bool | If true, blocking quality failures prevent feature commit. |
| `seed` | int or null | Required if any factor is stochastic. |

## 6. Canonical Input Frame

`FeatureInputStore` must convert `curated_market_bar` into a typed factor input frame:

| Column | Type | Source |
|---|---|---|
| `dataset_id` | string | `curated_market_bar.dataset_id` |
| `symbol` | string | `curated_market_bar.symbol` |
| `exchange` | string | `curated_market_bar.exchange` |
| `asset_class` | string | `curated_market_bar.asset_class` |
| `freq` | string | `curated_market_bar.freq` |
| `trading_date` | date | `curated_market_bar.trading_date` |
| `as_of` | datetime | `curated_market_bar.bar_end_time` |
| `bar_start_time` | datetime | `curated_market_bar.bar_start_time` |
| `open` | float64 | cast from decimal string |
| `high` | float64 | cast from decimal string |
| `low` | float64 | cast from decimal string |
| `close` | float64 | cast from decimal string |
| `volume` | float64 | cast from decimal string |
| `turnover` | float64 or null | cast from decimal string |
| `source_run_id` | string | lineage |

Sorting requirement:

```text
sort by symbol, as_of ascending before rolling/window operations
```

The factor layer must reject input frames missing required fields before computation starts.

## 7. Factor Semantics

### 7.1 Time Alignment

Each output row is valid at `as_of`, where:

```text
as_of = input bar_end_time
```

No factor may use bars where:

```text
bar_end_time > as_of
```

Forward returns, labels, or target variables must live in a separate label layer, not in the factor layer.

### 7.2 Warmup

If a factor requires `lookback_bars = N`, the first `N - 1` rows per symbol may be null. The system must either:

1. keep rows and set `warmup_complete = false`, or
2. drop rows only when `FactorRunConfig.drop_warmup = true`.

Default: keep rows and mark `warmup_complete`.

### 7.3 Frequency

Daily and minute factors share the same contract. Differences:

| Area | Daily | Minute |
|---|---|---|
| `as_of` | daily bar end timestamp | minute bar end timestamp |
| continuity checks | trading-day calendar | session/time-window calendar |
| warmup | count daily bars | count minute bars |
| output grain | symbol + day | symbol + minute |

### 7.4 Cross-Sectional Factors

MVP-1 can include simple cross-sectional ranks only if they depend solely on the current `as_of` slice.

Allowed:

```text
rank(ret_1) within same freq and same as_of
```

Not yet allowed:

```text
industry neutralization
market-cap neutralization
benchmark residualization
```

Those require reference datasets and separate data quality specs.

## 8. Storage Schema

### 8.1 `factor_run_manifest`

One row per factor execution.

| Column | Type | Notes |
|---|---|---|
| `factor_run_id` | text | Primary run id. |
| `feature_set_id` | text | Logical feature set. |
| `dataset_id` | text | Source dataset. |
| `freq` | text | Frequency. |
| `input_data_refs_json` | text | JSON array of refs. |
| `factor_versions_json` | text | `{factor_id: version}`. |
| `engine` | text | `polars`. |
| `execution_mode` | text | `lazy`, `streaming`, or `eager_debug`. |
| `polars_version` | text | Runtime Polars version. |
| `code_version` | text | Git SHA if available. |
| `status` | text | `CREATED`, `RUNNING`, `COMMITTED`, `FAILED`. |
| `started_at` | text | ISO timestamp. |
| `finished_at` | text | Nullable ISO timestamp. |
| `row_count_input` | integer | Input bars read. |
| `row_count_feature` | integer | Long feature rows written. |
| `row_count_snapshot` | integer | Snapshot rows written. |
| `quality_summary_json` | text | Aggregated quality metrics. |
| `error_code` | text | Nullable. |
| `error_message` | text | Nullable. |

### 8.2 `feature_table`

Long-format, append-only feature values.

| Column | Type | Notes |
|---|---|---|
| `factor_run_id` | text | Run lineage. |
| `feature_set_id` | text | Logical feature set. |
| `dataset_id` | text | Source dataset. |
| `symbol` | text | Instrument. |
| `freq` | text | Frequency. |
| `as_of` | text | Feature timestamp. |
| `factor_id` | text | Factor id. |
| `factor_version` | text | Factor version. |
| `output_field` | text | Output column. |
| `value_float` | double | Numeric value when applicable. |
| `value_string` | text | Non-numeric value when applicable. |
| `value_kind` | text | `float`, `string`, `bool`, `null`. |
| `warmup_complete` | boolean | Whether lookback window is ready. |
| `quality_flags_json` | text | Row-level warnings. |
| `input_data_ref` | text | Source data ref. |
| `created_at` | text | ISO timestamp. |

Uniqueness key:

```text
feature_set_id, dataset_id, symbol, freq, as_of, factor_id, factor_version, output_field
```

### 8.3 `feature_snapshot`

Strategy-friendly wide JSON view.

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | text | Stable id. |
| `feature_set_id` | text | Logical feature set. |
| `dataset_id` | text | Source dataset. |
| `symbol` | text | Instrument. |
| `freq` | text | Frequency. |
| `as_of` | text | Feature timestamp. |
| `features_json` | text | `{output_field: value}`. |
| `factor_run_ids_json` | text | Producing runs. |
| `input_data_refs_json` | text | Source refs. |
| `warmup_complete` | boolean | All included features warm. |
| `quality_flags_json` | text | Combined row warnings. |
| `feature_ref` | text | Logical ref to this snapshot row or slice. |
| `created_at` | text | ISO timestamp. |

### 8.4 `factor_quality_metric`

One row per metric per factor/output field.

| Column | Type | Notes |
|---|---|---|
| `factor_run_id` | text | Run lineage. |
| `factor_id` | text | Factor id. |
| `output_field` | text | Output field. |
| `metric_name` | text | Metric id. |
| `metric_value` | double | Numeric metric. |
| `metric_json` | text | Optional structured detail. |
| `severity` | text | `INFO`, `WARNING`, `ERROR`. |
| `created_at` | text | ISO timestamp. |

Required metrics:

```text
row_count
symbol_count
as_of_min
as_of_max
null_ratio
warmup_incomplete_count
duplicate_key_count
extreme_value_count
future_leakage_count
```

## 9. Polars Execution Rules

### 9.1 Lazy First

The runner should build a single lazy plan when possible:

```text
read bars -> cast columns -> sort -> with_columns(factor expressions) -> select output columns
```

Collecting intermediate DataFrames is only allowed in `eager_debug` mode or for adapters that document why lazy execution cannot express the computation.

### 9.2 Expression Style

Preferred pattern:

```python
pl.col("close").pct_change().over("symbol").alias("ret_1")
pl.col("close").rolling_mean(window_size=20).over("symbol").alias("ma_20")
```

The runner must sort by `symbol, as_of` before applying window expressions.

### 9.3 Streaming

`execution_mode = streaming` should call:

```python
lazy_frame.collect(engine="streaming")
```

However, rolling/window operations may not always be fully streaming. The manifest must record the requested execution mode and the actual observed behavior when this can be inspected.

Default for MVP-1:

```text
execution_mode = lazy
```

### 9.4 Python UDF Guardrail

Python UDF factors must be opt-in and marked in the manifest. They are acceptable for experiments but should not become the default path because they are harder to optimize, test, and port to a future stream engine.

## 10. Factor Registry

Registry requirements:

1. Reject duplicate `factor_id + version`.
2. Reject missing `input_fields` or `output_fields`.
3. Reject unsupported `compute_engine`.
4. Reject a requested factor if `freq` is not in `supported_freqs`.
5. Return spec and compute function together.
6. Support discovery by namespace, tag, output field, and supported frequency.

Minimal API draft:

```python
class FactorRegistry:
    def register(self, spec: FactorSpec, compute: FactorCompute) -> None:
        ...

    def get(self, factor_id: str, version: str | None = None) -> RegisteredFactor:
        ...

    def resolve_many(self, factor_ids: Sequence[str]) -> list[RegisteredFactor]:
        ...

    def list(self, *, namespace: str | None = None, tag: str | None = None) -> list[FactorSpec]:
        ...
```

## 11. Initial Built-In Factors

MVP-1 should implement a small but useful factor set:

| Factor | Inputs | Output | Lookback | Notes |
|---|---|---|---|---|
| `ret_1` | `close` | `ret_1` | 2 | Close-to-close return. |
| `log_ret_1` | `close` | `log_ret_1` | 2 | Log return. |
| `ma_5` | `close` | `ma_5` | 5 | Moving average. |
| `ma_20` | `close` | `ma_20` | 20 | Moving average. |
| `volatility_20` | `close` | `volatility_20` | 21 | Rolling std of returns. |
| `volume_change_5` | `volume` | `volume_change_5` | 6 | Volume momentum. |
| `hl_range` | `high`, `low`, `close` | `hl_range` | 1 | `(high - low) / close`. |

These factors are intentionally simple. Their real value is validating the contract, registry, Polars plan, feature store, and quality report.

## 12. Quality Gate

A factor run must fail before commit if:

1. Required input fields are missing.
2. Input data ref resolves to zero rows.
3. Any requested factor is unknown.
4. Any requested factor does not support the requested frequency.
5. Duplicate feature keys appear after computation.
6. `future_leakage_count > 0`.
7. `strict_quality = true` and a factor output exceeds its configured `max_null_ratio`.

Warnings should be recorded but not block commit by default:

```text
high_null_ratio
low_symbol_coverage
extreme_value_detected
warmup_rows_present
python_udf_used
streaming_fallback_possible
```

## 13. End-to-End Scenario

```text
Given:
  input_data_ref = duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d&adjustment=NONE&source_run_id=run-valid
  factors = ["ret_1", "ma_5", "hl_range"]
  feature_set_id = "basic_price_v1"

When:
  FactorRunService.run(config)

Then:
  factor_run_manifest contains one COMMITTED row
  feature_table contains long factor rows by symbol, freq, as_of, factor_id, output_field
  feature_snapshot contains one row per symbol, freq, as_of
  factor_quality_metric contains null_ratio, coverage, warmup, duplicate, and leakage metrics
  returned ref is duckdb://feature_snapshot?feature_set_id=basic_price_v1&factor_run_id=<id>
```

## 14. Tests

Minimum test coverage:

| Test | Assertion |
|---|---|
| `test_factor_spec_requires_outputs` | Missing output fields are rejected. |
| `test_registry_resolves_factor` | Registered factor returns spec and compute callable. |
| `test_polars_runner_computes_ret_1` | Return factor matches fixture expectation. |
| `test_runner_preserves_minute_as_of` | Minute factor output keeps minute timestamps. |
| `test_runner_rejects_unknown_factor` | Unknown factor writes no feature rows. |
| `test_feature_store_writes_manifest_and_snapshot` | Manifest, table, and snapshot are committed atomically. |
| `test_quality_report_detects_nulls_and_warmup` | Warmup/null metrics are recorded. |
| `test_factor_layer_does_not_read_csv_path` | Input must be `DataRef`, not raw file path. |

## 15. Implementation Order

Recommended next branches:

```text
feature/factor-contracts
feature/polars-factor-runner
feature/feature-store
feature/factor-quality-report
feature/research-cli
```

Step order:

1. Add `quant_research.factors.contracts`.
2. Add `FactorRegistry` and built-in factor specs.
3. Add `FeatureInputStore` for `DataRef -> pl.LazyFrame`.
4. Add `PolarsFactorRunner` for expression factors.
5. Add DuckDB feature tables and atomic commit.
6. Add quality metrics.
7. Add CLI or script-level end-to-end demo.

## 16. Decoupling Rules

1. Factor definitions must not import DuckDB.
2. Strategy/replay code must not import Polars.
3. Feature consumers must use `FeatureSnapshot` or `DataRef`.
4. Physical SQL construction must stay inside store adapters.
5. Computation code must not know CSV paths.
6. Quality gates must run before feature table commit.
7. All runs must be reproducible from manifest + input data refs + factor versions.

## 17. Open Decisions For Later

These are intentionally deferred:

1. Whether to materialize a wide `feature_matrix` table for faster model training.
2. Whether to export feature snapshots to partitioned Parquet.
3. Whether to use Polars streaming as default for very large minute datasets.
4. How to represent industry/benchmark/reference datasets for neutralized factors.
5. How to share the same factor definitions with a future live/streaming engine.
