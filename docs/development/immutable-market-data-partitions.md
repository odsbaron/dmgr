# Immutable Market-Data Partitions

## Purpose

The immutable market-data path turns local date-partitioned CSV or Parquet bars into reproducible research assets. It separates three identities:

```text
import run       = which source file was processed
daily partition = canonical bars committed for one trading date
snapshot set    = exact ordered daily partitions used by a research run
```

The legacy `DataIngestionService` and its import-run refs remain available. New production-oriented research should use the immutable path.

## Dataset definition

`MarketDatasetDefinition` versions the semantics shared by every daily partition:

```text
dataset_id
version
name
asset_class
freq
adjustment
calendar_id
timezone
bar_timestamp_convention
```

`bar_timestamp_convention` is either `START_TIME` or `END_TIME`. The normalizer converts both representations into explicit UTC `bar_start_time` and `bar_end_time` values. Session membership and expected bar counts are intentionally not inferred here.

Changing frequency, adjustment, asset class, calendar, timezone, or timestamp convention requires a new definition version. Registering different content under an existing dataset id/version is rejected with `DEFINITION_CONFLICT`.

## Date-partition source layout

The standard local layout is:

```text
market-data/
  dataset_id=ashare-1m/
    version=v1/
      trading_date=2026-07-01/
        bars.parquet
      trading_date=2026-07-02/
        bars.parquet
```

One `MarketDataSourceSpec` declares exactly one `trading_date`. Required canonical mappings are:

```text
symbol
exchange
open
high
low
close
volume
```

Daily bars additionally require `date`. Intraday bars require `datetime` or `bar_start_time`. `turnover` is optional. Source-level metadata is explicit:

```text
source_id
dataset_id
dataset_version
known_at
source_data_cutoff
```

Both point-in-time timestamps must be timezone-aware, `source_data_cutoff <= known_at`, and no committed bar may end after the source cutoff. Filesystem modification time is never treated as research lineage.

## Ingestion and canonical identity

Use `ImmutableMarketDataIngestionService` with a registered definition and source specification. The service supports CSV and Parquet without additional dependencies.

Canonical partition hashing includes:

- dataset definition hash;
- declared trading date;
- `known_at` and `source_data_cutoff`;
- bars sorted by symbol and bar start time;
- normalized timestamps and decimal OHLCV values.

It excludes file format, source path, file hash, import run id, source row id, and raw ref. Equivalent CSV and Parquet representations therefore produce the same partition id even when numeric serialization differs.

The logical key is:

```text
dataset_id + dataset_version + trading_date
```

Commit behavior is:

```text
same key + same content hash      -> reuse committed partition
same key + different content hash -> IMMUTABLE_PARTITION_CONFLICT
```

Conflicting imports and quality failures remain recorded in `market_data_import_run` and `bar_quality_issue`; committed partition metadata and bars are not replaced.

## Snapshot sets and refs

After daily ingestion, create an exact set with explicitly requested trading dates:

```python
snapshot_set = store.create_market_data_snapshot_set(
    dataset_id="ashare-1m",
    dataset_version="v1",
    trading_dates=trading_dates,
)
```

Every requested date must have one committed partition. The stable research ref is:

```text
duckdb://curated_market_bar?snapshot_set_id=<id>
```

Its set hash pins the definition hash and ordered `(trading_date, partition_id, content_hash)` items. Reading this ref joins only the pinned partition ids, so later imports cannot expand or alter the run input.

## DuckDB tables

The immutable path adds:

```text
market_data_definition
market_data_import_run
market_data_partition
market_data_snapshot_set_manifest
market_data_snapshot_set_item
```

`curated_market_bar.market_data_partition_id` is nullable. Immutable rows populate it; legacy rows remain null. Existing databases receive the column through an additive migration.

## Research pipeline behavior

For an exact market-data ref, `ResearchPipeline` performs:

```text
resolve dataset definition and exact dates
  -> resolve optional Universe
  -> read only pinned partitions
  -> validate market dates and asset class against Universe
  -> preserve legal pre-start lookback
  -> compute and crop factors
  -> commit features and exact lineage
```

The feature manifest records:

```text
market_data_ref
market_dataset_version
market_data_definition_hash
market_data_snapshot_set_hash
```

Legacy refs remain supported and leave these fields null. Snapshot-set refs cannot be combined with legacy dataset or source-run filters.

## Deferred semantics

This subsystem proves which bars were used, but does not claim that all expected bars exist. Companion Calendar and InstrumentDailyStatus assets now exist, but expected-slot expansion and pipeline coverage enforcement remain a separate change:

- expected-minute coverage and strict coverage gates;
- DuckDB/Arrow query pushdown;
- instrument aliases and corporate-action identity.

Until the coverage gate consumes those companion assets, a missing Universe member bar remains ambiguous and no strict expected/actual minute ratio should be reported.
