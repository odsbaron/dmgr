# Daily Portfolio Backtest

The daily backtest lane introduces two one-way domains:

```text
factor or model producer
  -> AlphaScore(as_of, available_at, source_ref)
  -> EqualWeightPortfolioBuilder
  -> TargetWeight
  -> DailyBacktestPipeline
  -> fills / positions / NAV / metrics / manifest
```

`quant_research.signals` owns scores and portfolio construction.
`quant_research.backtest` owns simulated execution, costs, accounting, analytics,
and result persistence. Neither package imports the factor, label, dataset, or
training implementations.

## Time semantics

`as_of` is the timestamp to which the score belongs. `available_at` is the earliest
time the score can be consumed. The MVP execution convention is
`NEXT_ELIGIBLE_DAILY_OPEN`: a target can only trade on a bar whose end is strictly
after `as_of` and whose start is not before `available_at`.

## Portfolio construction

`EqualWeightPortfolioBuilder` groups scores by dataset, frequency, and `as_of`, then
ranks descending by score with symbol as the deterministic tie-breaker. It supports
Top-K and top-quantile long-only selection. Target weights sum to the configured
gross exposure, which is at most one.

## Simulation

`DailyBacktestPipeline` accepts a `DailyBacktestRequest` containing stable target and
market-data refs plus in-memory domain records supplied by the caller. On each
rebalance date it:

1. values current positions at the daily open or the latest known mark;
2. computes integer target quantities;
3. sells reductions before buying additions;
4. applies the injected `TradingEligibility` policy and proportional costs;
5. marks positions at the daily close and records `NAV = cash + market value`.

The default eligibility implementation requires positive open price and volume.
Coverage and DailyStatus integrations should implement the same narrow eligibility
protocol rather than add their rules to the simulator.

## Persistence

`LocalDuckDBBacktestStore` writes:

- `backtest_run_manifest`
- `backtest_fill`
- `backtest_position`
- `backtest_nav`
- `backtest_metric`

The manifest records the stable input refs, execution and cost configuration, code
version, config hash, optional Universe/Calendar/DailyStatus/Coverage refs, and row
counts. Repeating a committed run id with an identical config reuses the result;
changing the config raises `BACKTEST_RUN_CONFLICT` without replacing the original.

This MVP deliberately excludes live trading, partial fills, price limits, corporate
actions, leverage, shorting, and minute-level execution.
