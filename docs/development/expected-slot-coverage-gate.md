# Expected-slot coverage gate

`quant_research.coverage` owns the completeness decision that sits between immutable market inputs and downstream research. It does not repair bars and it does not modify Calendar, Universe, or DailyStatus assets.

## Input contract

A `CoverageRunRequest` pins four point-in-time refs:

```text
MarketDataRef × CalendarRef × UniverseRef × DailyStatusRef
```

It also fixes the date range, frequency, `BAR_START`/`BAR_END` timestamp convention, policy, and minimum ratio. `CoveragePipeline` resolves each ref through its owning resolver and rejects incompatible calendar, version, timezone, asset-class, date, or frequency identities.

## Expected-slot semantics

- `FULL_SESSION` uses every Calendar session independently, so a lunch break is never filled.
- `NO_BARS` expects no bars and therefore models suspension without a false missing-data error.
- `CUSTOM_INTERVALS` uses only the explicitly declared intervals.
- `UNKNOWN` or a missing DailyStatus row is never guessed as active; it produces a blocking issue.
- Intraday windows are identified by their start or end timestamp according to the request.
- Daily bars use `(symbol, trading_date)` as their comparison identity, independent of vendor wall-clock timestamps.

## Outputs and consumption

Each run atomically writes:

```text
duckdb://coverage_run_manifest?coverage_run_id=...
duckdb://coverage_metric?coverage_run_id=...
duckdb://coverage_issue?coverage_run_id=...
```

Metrics are available at run, date, and symbol-date scope and record expected, actual, matched, missing, unexpected, and ratio values.

`STRICT` requires known semantics, perfect slot matching, and the configured minimum ratio. `WARNING` preserves missing/unexpected facts but permits exploratory consumption when there are no ERROR issues. Unknown semantics block both modes.

`CoverageConsumptionGate` is the only integration surface required by `ResearchPipeline`. A research request may supply `coverage_report_ref`; the pipeline asks the injected gate whether the report is consumable before computing factors and never queries Coverage-owned tables directly.
