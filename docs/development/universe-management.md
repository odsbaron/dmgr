# Research Universe Management

## Purpose

The Universe subsystem manages the instruments included in research on each trading date. It is independent from market-data availability: an instrument can remain a Universe member even when no bar rows are present.

The first implementation supports immutable, member-only daily snapshots imported from local CSV or Parquet files. It is suitable for A-share minute and daily research while treating `instrument_id` as an opaque identifier so other assets can use the same contracts.

## Boundaries

The subsystem answers only this question:

```text
Which instrument ids belong to this research Universe on trading date D?
```

It does not define:

- whether an instrument is suspended or expected to produce bars;
- whether an order can be submitted;
- intraday membership changes;
- automatic selection rules;
- instrument aliases or corporate-action identity.

Those concerns require separate DailyStatus, execution, rule-build, and instrument-reference capabilities.

## Definition and daily files

One `UniverseDefinition` fixes the Universe id/version, asset class, calendar, timezone, selection cutoff, and construction mode. One local file contains the members for exactly one `trading_date`.

Recommended layout:

```text
universe/
  universe_id=ashare-research/
    version=v1/
      trading_date=2026-07-01/
        members.parquet
      trading_date=2026-07-02/
        members.parquet
```

Required canonical field:

```text
instrument_id
```

Optional canonical fields:

```text
trading_date
weight
rank
inclusion_tags
```

`UniverseSourceSpec.field_mapping` maps canonical names to source columns. Metadata that cannot safely be inferred from rows is supplied explicitly:

```text
universe_id
universe_version
trading_date
known_at
source_data_cutoff
```

Filesystem modification time is never used as `known_at`.

## Immutability and hashing

The logical partition key is:

```text
universe_id + universe_version + trading_date
```

Members are normalized and sorted before hashing. CSV and Parquet representations with the same canonical members therefore produce the same snapshot content hash.

Reimport behavior:

```text
same logical key + same content hash      -> reuse committed snapshot
same logical key + different content hash -> IMMUTABLE_PARTITION_CONFLICT
```

A `UniverseSnapshotSet` pins an ordered set of daily snapshots. Research runs consume its stable ref:

```text
duckdb://universe_member?snapshot_set_id=<id>
```

The ref identifies concrete contents rather than looking up the latest members by date.

## Quality gate

Strict imports block on:

- empty snapshots;
- duplicate or empty instrument ids;
- member/file trading-date mismatch;
- invalid weights or ranks;
- naive point-in-time timestamps;
- `source_data_cutoff > known_at`;
- `known_at` after the definition's daily selection cutoff;
- definition/source id mismatch.

Import runs and quality issues are retained even when no snapshot is committed.

## Research pipeline behavior

`ResearchRunRequest.universe_ref` is optional for backward-compatible time-series runs. When present, `ResearchPipeline` requires a configured `UniverseResolver` and performs:

```text
resolve exact daily memberships
  -> validate output market-data dates and asset class
  -> keep bars for the union of required members
  -> retain bars before as_of_start as legal lookback history
  -> compute time-series factors
  -> crop to as_of_start/as_of_end
  -> inner join output rows with membership for each trading_date
  -> commit feature assets and Universe lineage
```

If both `universe_ref` and static `symbols` are supplied, `symbols` is only a debugging intersection. It is not persisted as an authoritative Universe definition.

The factor-run manifest records:

```text
universe_ref
universe_id
universe_version
universe_definition_hash
universe_snapshot_set_hash
```

Legacy runs leave these fields null instead of inventing membership lineage from observed bars.

## Deferred coverage semantics

This implementation deliberately does not claim strict symbol/minute coverage. Independent Calendar and InstrumentDailyStatus sources now exist, but the expected-slot coverage gate that combines them with Universe membership remains a separate change.

Until that integration exists:

- Universe membership remains authoritative;
- missing bars do not remove a member from the resolved Universe;
- no `expected bars / actual bars` coverage ratio is produced;
- cross-sectional factor operators remain out of scope.
