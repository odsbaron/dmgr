# Market Calendar and Instrument Daily Status

## Separation of responsibilities

The three daily inputs answer different questions:

```text
Universe          -> which instruments belong to research on date D?
Market Calendar   -> is date D open, and which local sessions exist?
Daily Status      -> what market state and bar expectation applies to instrument I on D?
```

No subsystem infers another from observed market-data rows. A suspended instrument remains a Universe member and can have `SUSPENDED + NO_BARS` status even when it has no bars.

## Market calendar input

`MarketCalendarDefinition` versions the calendar id, name, and IANA timezone. One CSV or Parquet file declares exactly one `calendar_date`.

Recommended layout:

```text
calendar/
  calendar_id=xshg-xshe/
    version=v1/
      calendar_date=2026-07-07/
        sessions.parquet
```

Canonical fields are:

```text
is_trading_day     required
calendar_date      optional row-level consistency check
session_id         required for an open-day session
session_start      local wall-clock time
session_end        local wall-clock time
session_kind       optional, defaults to REGULAR
```

A regular A-share input can contain:

```text
morning    09:30:00  11:30:00
afternoon  13:00:00  15:00:00
```

The lunch break is not an open interval. These times are input data, not engine constants. A closed date is represented by one `is_trading_day=false` row with empty session fields.

Strict validation rejects inconsistent open/closed rows, open days without sessions, closed days with sessions, duplicate ids, missing or invalid times, overlaps, date mismatches, naive point-in-time values, and source cutoffs after `known_at`.

## Daily status input

`DailyStatusDefinition` versions the status dataset, asset class, calendar id/version, and timezone. One CSV or Parquet file contains statuses for exactly one `trading_date`.

Recommended layout:

```text
daily-status/
  status_id=ashare-daily-status/
    version=v1/
      trading_date=2026-07-07/
        status.parquet
```

Canonical fields:

```text
instrument_id
market_state
bar_expectation
trading_date       optional row-level consistency check
custom_intervals   optional JSON/list or start-end|start-end text
```

Supported states are `ACTIVE`, `SUSPENDED`, `NOT_LISTED`, and `UNKNOWN`. Supported expectations are:

```text
FULL_SESSION     use the calendar sessions
NO_BARS          expect zero bars
CUSTOM_INTERVALS use the row's explicit local intervals
UNKNOWN          make no completeness claim
```

`is_tradable` is deliberately absent. Market state, expected data, and strategy/order eligibility are separate concepts.

## Immutability and refs

Both subsystems use the same identity pattern as market data and Universe:

```text
logical key = id + version + date
same canonical content      -> reuse
different canonical content -> IMMUTABLE_PARTITION_CONFLICT
```

Exact set refs are:

```text
duckdb://market_calendar_day?snapshot_set_id=<id>
duckdb://instrument_daily_status?snapshot_set_id=<id>
```

Set hashes pin the definition hash and ordered date/snapshot/content-hash items. CSV and Parquet representations with equivalent canonical content produce the same daily snapshot identity.

## Current boundary

Calendar and DailyStatus ingestion, persistence, refs, and resolution are implemented. They are not yet consumed by `ResearchPipeline` to expand expected minute slots. The next coverage change must join:

```text
Calendar × Universe membership × DailyStatus × frequency/timestamp convention
```

Until that gate is added, these assets state expectations reproducibly but do not yet block a factor run for missing minutes.
