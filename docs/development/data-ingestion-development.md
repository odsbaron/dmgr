# Data Ingestion Development Spec

> Branch: `feature/data-ingestion-spec`  
> Worktree: `.worktrees/data-ingestion-spec`  
> Scope: CSV / Parquet K line inputs -> normalized Bar schema -> validation -> `data/research.duckdb`.

## 1. Purpose

Data ingestion is the first implementation lane of `quant-research-mvp0`. Its job is to make external K line data enter the local research system in a controlled, auditable, and repeatable way.

It does not compute factors. It does not run strategies. It does not touch orders, accounts, gateways, or simulated fills.

The output of this lane is:

```text
duckdb://curated_market_bar?dataset_id=...&freq=...&trading_date=...&symbol=...
```

That `data_ref` becomes the input boundary for the factor-computation lane.

## 2. Upstream References

Canonical planning docs live outside this worktree:

- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/proposal.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/specs/kline-dataset/spec.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/specs/factor-computation/spec.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/openspec/changes/kline-batch-research-framework/specs/research-pipeline/spec.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/specs/2026-07-07-kline-batch-research-framework-design.md`
- `/Users/dsou/Desktop/workshop/量化仓库学习/docs/superpowers/plans/2026-07-07-kline-batch-research-framework-implementation.md`

This document narrows those specs into a development design for the data-ingestion lane.

## 3. Worktree Strategy

The base repository is:

```text
/Users/dsou/Desktop/workshop/量化仓库学习/quant-research-mvp0
```

Current worktree:

```text
/Users/dsou/Desktop/workshop/量化仓库学习/quant-research-mvp0/.worktrees/data-ingestion-spec
```

Recommended parallel lanes:

| Lane | Branch | Responsibility | Depends on |
|---|---|---|---|
| Data ingestion | `feature/data-ingestion-spec` | SourceSpec, readers, normalization, validation, DuckDB writes | main baseline |
| DuckDB store | `feature/duckdb-store` | Store adapter, schema creation, `data_ref` parser | data-ingestion contracts |
| Factor registry | `feature/factor-registry` | FactorSpec, registry, built-in factor metadata | contracts |
| Batch factors | `feature/batch-factor-compute` | Polars compute runner and feature writes | duckdb-store, factor-registry |
| Pipeline CLI | `feature/research-pipeline-cli` | Typer commands and end-to-end runner | all prior lanes |

Branch merge order:

```text
main
  <- feature/data-ingestion-spec
  <- feature/duckdb-store
  <- feature/factor-registry
  <- feature/batch-factor-compute
  <- feature/research-pipeline-cli
```

## 4. Data Flow

```text
SourceSpec
  -> ImportRun(CREATED)
  -> read source file/table
  -> raw_kline_import
  -> field mapping
  -> BarRecord normalization
  -> K line validation
  -> DuckDB transaction
  -> curated_market_bar
  -> bar_quality_issue
  -> ImportRun(COMMITTED or FAILED)
  -> data_ref
```

The commit boundary is important: `curated_market_bar` is only visible to factor computation after validation passes or repair mode is explicitly enabled.

## 5. SourceSpec

`SourceSpec` describes how an external data source should be read and interpreted.

Required fields:

| Field | Type | Rule |
|---|---|---|
| `source_id` | string | Stable source name, such as `local_csv_tdx_daily`. |
| `dataset_id` | string | Logical dataset, such as `demo` or `a_share_daily`. |
| `source_type` | enum | `CSV`, `PARQUET`, `DUCKDB_TABLE` reserved for later. |
| `path` | string | Local file or directory path for MVP-0. |
| `freq` | enum | `1m`, `5m`, `15m`, `30m`, `60m`, `1d`. |
| `timezone` | string | Example: `Asia/Shanghai`. |
| `adjustment` | enum | `NONE`, `FORWARD`, `BACKWARD`. |
| `field_mapping` | map | Maps source columns to canonical Bar fields. |
| `symbol_mapping` | map or null | Optional source symbol to canonical symbol mapping. |
| `calendar_id` | string | Example: `cn_stock_simple`, `cn_future_simple`. |
| `strict_mode` | bool | If true, validation failures block curated writes. |
| `repair_mode` | bool | If true, allowed repairs must be recorded. |

Example:

```yaml
source_id: local_csv_fixture_daily
dataset_id: fixture-daily
source_type: CSV
path: tests/fixtures/bars_daily.csv
freq: 1d
timezone: Asia/Shanghai
adjustment: NONE
calendar_id: cn_stock_simple
strict_mode: true
repair_mode: false
field_mapping:
  symbol: symbol
  exchange: exchange
  date: date
  open: open
  high: high
  low: low
  close: close
  volume: volume
  turnover: turnover
symbol_mapping: {}
```

## 6. ImportRun Lifecycle

Each ingest execution creates an `import_run_id`.

States:

| State | Meaning |
|---|---|
| `CREATED` | Import request accepted and source hash computed. |
| `READING` | Source rows are being read. |
| `NORMALIZING` | Rows are mapped into BarRecord candidates. |
| `VALIDATING` | Quality checks are running. |
| `COMMITTING` | DuckDB transaction is writing accepted rows and reports. |
| `COMMITTED` | Curated data and reports are queryable. |
| `FAILED` | Import failed; failure reason is recorded. |

State transition rule:

```text
CREATED -> READING -> NORMALIZING -> VALIDATING -> COMMITTING -> COMMITTED
                                      \                         /
                                       -> FAILED <-------------
```

The system must write a failed import record if failure happens after `CREATED`.

## 7. DuckDB Tables

### 7.1 `raw_kline_import`

Stores import metadata and raw row references.

| Column | Type | Notes |
|---|---|---|
| `import_run_id` | text | Primary run id. |
| `source_id` | text | From SourceSpec. |
| `dataset_id` | text | Logical dataset. |
| `source_type` | text | `CSV` or `PARQUET`. |
| `source_path` | text | Local path. |
| `source_file_hash` | text | Hash of source content or manifest. |
| `source_row_id` | text | Row number or Parquet row group reference. |
| `raw_payload_json` | text | Raw row snapshot for audit; bounded for MVP-0. |
| `created_at` | timestamp | System time. |

### 7.2 `import_run`

Stores run-level lifecycle and idempotency metadata.

| Column | Type | Notes |
|---|---|---|
| `import_run_id` | text | Primary key. |
| `dataset_id` | text | Logical dataset. |
| `source_id` | text | SourceSpec id. |
| `freq` | text | Supported frequency. |
| `adjustment` | text | Adjustment mode. |
| `source_file_hash` | text | Used for idempotency. |
| `status` | text | ImportRun state. |
| `row_count_raw` | integer | Rows read. |
| `row_count_curated` | integer | Rows committed. |
| `issue_count` | integer | Quality issue count. |
| `started_at` | timestamp | System time. |
| `finished_at` | timestamp | Nullable. |
| `error_code` | text | Nullable. |
| `error_message` | text | Nullable. |

### 7.3 `curated_market_bar`

Stores normalized bars.

Required query columns:

```text
dataset_id
symbol
exchange
asset_class
freq
trading_date
bar_start_time
bar_end_time
adjustment
```

Required value columns:

```text
open
high
low
close
volume
turnover
source
source_run_id
source_row_id
raw_ref
created_at
```

Uniqueness key:

```text
dataset_id, symbol, freq, adjustment, bar_start_time
```

### 7.4 `bar_quality_issue`

Stores data quality issues.

Required columns:

```text
issue_id
import_run_id
dataset_id
symbol
freq
trading_date
bar_start_time
issue_code
severity
message
raw_ref
created_at
```

Issue codes for MVP-0:

```text
MISSING_REQUIRED_FIELD
UNSUPPORTED_FREQ
DUPLICATE_BAR
INVALID_OHLC
NEGATIVE_VOLUME
INVALID_TIMESTAMP
MISSING_BAR_WINDOW
MISSING_LINEAGE
```

## 8. Idempotency

Idempotency key:

```text
dataset_id + source_id + source_file_hash + freq + adjustment
```

Rules:

1. If the same idempotency key was already committed, a rerun must return the existing `data_ref`.
2. If the same idempotency key is currently running, a new run must fail with `IMPORT_ALREADY_RUNNING`.
3. If a previous run failed, rerun is allowed and creates a new `import_run_id`.
4. Curated writes must not duplicate the uniqueness key.

## 9. Validation Gate

Strict mode:

- Any `ERROR` issue blocks writes to `curated_market_bar`.
- `WARNING` issues are recorded but do not block.

Repair mode:

- Allowed repairs must be explicit and recorded in `bar_quality_issue`.
- MVP-0 allowed repair: trim whitespace, normalize symbol case, parse numeric strings.
- MVP-0 disallowed repair: forward-fill missing bars, alter OHLC prices, infer missing volume.

## 10. DataRef Format

Successful import returns:

```text
duckdb://curated_market_bar?dataset_id=<dataset>&freq=<freq>&source_run_id=<import_run_id>
```

Optional narrower refs:

```text
duckdb://curated_market_bar?dataset_id=<dataset>&freq=1d&trading_date=2026-07-07&symbol=000001.SZ
```

Parsing rules:

1. Scheme must be `duckdb`.
2. Host/path is the logical table name.
3. Query parameters are equality filters.
4. Domain code must not concatenate raw SQL.

## 11. Reader Interfaces

The implementation should expose small interfaces:

```python
class KLineReader(Protocol):
    def read_rows(self, spec: SourceSpec) -> Iterable[RawKLineRow]:
        ...


class BarNormalizer(Protocol):
    def normalize(self, row: RawKLineRow, spec: SourceSpec) -> BarRecord:
        ...


class ResearchStore(Protocol):
    def begin_import(self, spec: SourceSpec) -> ImportRun:
        ...

    def commit_bars(
        self,
        run: ImportRun,
        bars: Iterable[BarRecord],
        issues: Iterable[QualityIssue],
    ) -> DataRef:
        ...
```

These interfaces are implementation guides, not public cross-package contracts yet.

## 12. Tests

Minimum tests:

1. CSV daily fixture imports into `curated_market_bar`.
2. CSV minute fixture preserves minute `bar_start_time`.
3. Duplicate bar is recorded as `DUPLICATE_BAR`.
4. Invalid OHLC blocks curated write in strict mode.
5. Rerun with same source hash returns existing `data_ref`.
6. Failed import writes `import_run.status = FAILED`.
7. DataRef parser rejects non-DuckDB refs.

## 13. Handoff Criteria

Data ingestion lane is ready for merge when:

- `docs/development/data-ingestion-development.md` is complete.
- SourceSpec, ImportRun, DuckDB tables, idempotency, validation gate, and data_ref semantics are implemented.
- Fixture tests pass for daily and minute data.
- No factor code depends on raw file paths.
