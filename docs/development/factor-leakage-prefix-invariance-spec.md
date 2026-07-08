# Factor Leakage Prefix Invariance Spec

> Scope: dynamic forward-leakage detection by recomputing factors on historical prefixes.
> Status: design spec.
> Key decision: `as_of` is the factor value timestamp, not the data availability timestamp.

## 1. Purpose

The current quality layer can detect future-looking factors only when the factor declares metadata such as:

```python
quality_rules={
    "forward_bars": 1,
    "causal": False,
}
```

This spec adds a dynamic detector that does not rely only on metadata. It tests whether a historical factor value changes after future rows are appended to the input.

Core invariant:

```text
For a causal factor, the value at as_of = t must be identical when computed on:

1. input rows with as_of <= t
2. the full input rows, including rows with as_of > t
```

If the historical value changes, the factor computation has observed future data or full-sample state.

This is a prefix-invariance test:

```text
compute(prefix(input, cutpoint=t))[rows <= t]
must equal
compute(full_input)[rows <= t]
```

## 2. `as_of` Semantics

`as_of` means:

```text
The timestamp the factor value belongs to.
```

For K-line factors:

```text
as_of = the bar timestamp represented by the feature row
```

For the current bar adapter and factor layer, the preferred K-line default is:

```text
as_of = input bar_end_time
```

For event-style data, `as_of` should be the event timestamp that the computed value belongs to.

`as_of` does not mean:

```text
the earliest time this value is tradable
the earliest time all inputs are available
the system write time
the wall-clock execution time
```

Those concepts need separate fields.

Future fields:

| Field | Meaning |
|---|---|
| `available_at` | Earliest timestamp the value can be consumed by a strategy. |
| `input_window_start` | Earliest input timestamp used by the output row. |
| `input_window_end` | Latest input timestamp used by the output row. |
| `computed_at` | System time when the value was produced. |

This spec intentionally keeps `as_of` as the value timestamp because feature tables, snapshots, labels, and model matrices need a stable alignment key.

## 3. Leakage Definition

A factor output is prefix-invariant if adding future rows does not change already-produced historical values.

Given:

```text
D_full = all input rows sorted by symbol, as_of
D_prefix(t) = rows where as_of <= t
F = factor computation
```

For each output field and comparison key:

```text
key = dataset_id, symbol, freq, as_of, factor_id, output_field
```

The detector compares:

```text
F(D_prefix(t))[key]
F(D_full)[key]
```

If values differ for any key where `as_of <= t`, record a prefix-invariance violation.

Violation examples:

```python
# Future return.
pl.col("close").shift(-1) / pl.col("close") - 1.0

# Centered rolling window.
pl.col("close").rolling_mean(window_size=5, center=True)

# Full-sample normalization.
(pl.col("close") - pl.col("close").mean()) / pl.col("close").std()

# Full-sample max/min leakage.
pl.col("close") / pl.col("close").max()
```

Non-violation examples:

```python
# Historical return.
pl.col("close").pct_change(1).over("symbol")

# Trailing rolling mean.
pl.col("close").rolling_mean(window_size=5).over("symbol")
```

## 4. Component Boundary

Create a detector outside FeatureStore:

```text
quant_research.factors.leakage.PrefixInvarianceLeakageDetector
```

Responsibilities:

| Component | Responsibility |
|---|---|
| `PrefixInvarianceLeakageDetector` | Recompute factors on prefix inputs and compare historical outputs. |
| `PolarsFactorRunner` | Compute factor outputs for full and prefix frames. |
| `FactorRegistry` | Resolve factor specs and compute functions. |
| `FactorQualityAnalyzer` | Continue computing table-level metrics such as null ratio and duplicate keys. |
| `LocalDuckDBFeatureStore` | Persist final quality metrics and manifest status. |

Non-responsibilities:

| Component | Must not do |
|---|---|
| Detector | Persist DuckDB rows directly. |
| Detector | Mutate factor specs. |
| Detector | Decide strategy/training consumption. |
| FeatureStore | Recompute factors. |
| OperatorRegistry | Run dynamic leakage probes. |

## 5. API Draft

### 5.1 Probe Config

```python
class CutpointSelectionMode(StrEnum):
    EVENLY_SPACED = "evenly_spaced"
    PERIOD_END = "period_end"
    EXPLICIT = "explicit"


class CompareWindowMode(StrEnum):
    TAIL_BARS = "tail_bars"
    ALL_HISTORY = "all_history"


@dataclass(frozen=True)
class PrefixProbeConfig:
    enabled: bool = True
    cutpoint_mode: CutpointSelectionMode = CutpointSelectionMode.EVENLY_SPACED
    cutpoint_count: int = 5
    explicit_cutpoints: tuple[str, ...] = ()
    period: str | None = None
    min_prefix_rows: int = 20
    compare_window_mode: CompareWindowMode = CompareWindowMode.TAIL_BARS
    compare_tail_bars: int = 20
    min_compare_rows: int = 1
    rtol: float = 1e-9
    atol: float = 1e-12
    nulls_equal: bool = True
    max_examples: int = 20
```

Field semantics:

| Field | Meaning |
|---|---|
| `enabled` | Turns the dynamic prefix probe on or off. |
| `cutpoint_mode` | Strategy for selecting historical cutpoints. |
| `cutpoint_count` | Maximum number of cutpoints to sample when mode is `EVENLY_SPACED`. |
| `explicit_cutpoints` | User-provided cutpoints when mode is `EXPLICIT`. |
| `period` | Calendar bucket for `PERIOD_END`, such as `week`, `month`, or `quarter`. |
| `min_prefix_rows` | Minimum input rows required before a cutpoint can be probed. |
| `compare_window_mode` | Strategy for choosing rows to compare before each cutpoint. |
| `compare_tail_bars` | Number of historical rows per symbol to compare near each cutpoint. |
| `min_compare_rows` | Minimum comparable rows required for a probe result to be meaningful. |
| `rtol` | Relative tolerance for numeric comparison. |
| `atol` | Absolute tolerance for numeric comparison. |
| `nulls_equal` | Treat null/null and NaN/NaN as equal. |
| `max_examples` | Maximum changed-value examples to retain in metric JSON. |

Recommended profiles:

```python
DAILY_PREFIX_PROBE = PrefixProbeConfig(
    cutpoint_count=5,
    min_prefix_rows=60,
    compare_tail_bars=30,
)

MINUTE_PREFIX_PROBE = PrefixProbeConfig(
    cutpoint_count=8,
    min_prefix_rows=500,
    compare_tail_bars=120,
)

FAST_CI_PREFIX_PROBE = PrefixProbeConfig(
    cutpoint_count=2,
    min_prefix_rows=30,
    compare_tail_bars=20,
)

DEEP_AUDIT_PREFIX_PROBE = PrefixProbeConfig(
    cutpoint_count=12,
    min_prefix_rows=120,
    compare_window_mode=CompareWindowMode.ALL_HISTORY,
    compare_tail_bars=0,
)
```

### 5.2 Probe Example

```python
@dataclass(frozen=True)
class PrefixLeakageExample:
    cutpoint: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    factor_id: str
    output_field: str
    prefix_value: object
    full_value: object
```

### 5.3 Probe Report

```python
@dataclass(frozen=True)
class PrefixLeakageReport:
    factor_run_id: str
    feature_set_id: str
    checked_cutpoint_count: int
    compared_value_count: int
    violation_count: int
    examples: tuple[PrefixLeakageExample, ...]
```

### 5.4 Detector Interface

```python
class PrefixInvarianceLeakageDetector:
    def analyze(
        self,
        *,
        input_frame: pl.LazyFrame,
        config: FactorRunConfig,
        runner: PolarsFactorRunner,
        resolved_factors: tuple[RegisteredFactor, ...],
        probe_config: PrefixProbeConfig = PrefixProbeConfig(),
    ) -> PrefixLeakageReport:
        raise NotImplementedError
```

The detector accepts `input_frame`, not `FeatureValue`, because it must recompute factors from source inputs.

## 6. Algorithm

### 6.1 Full Computation

Compute the full factor frame once:

```text
full_factor_frame = runner.run(input_frame, config)
```

Collect only the required columns:

```text
dataset_id
symbol
freq
as_of
<factor output fields>
```

### 6.2 Cutpoint Selection

Use sorted unique `as_of` timestamps from `input_frame`.

Reject cutpoints that do not satisfy:

```text
prefix row count >= min_prefix_rows
cutpoint is not the final as_of
```

Then select cutpoints according to `PrefixProbeConfig.cutpoint_mode`.

#### 6.2.1 `EVENLY_SPACED`

Sample deterministic cutpoints across the usable history:

```text
if usable_cutpoints <= cutpoint_count:
    use all usable cutpoints
else:
    choose approximately evenly spaced cutpoints
```

No random sampling in MVP-1. Determinism matters for reproducible manifests.

Example:

```text
usable as_of count = 250
cutpoint_count = 5
selected approximate positions = 20%, 40%, 60%, 80%, 100% of usable range
```

The final global `as_of` must still be excluded because appending no future rows cannot test leakage.

#### 6.2.2 `PERIOD_END`

`PERIOD_END` selects the last available `as_of` in each configured calendar bucket.

Supported MVP-1 periods:

```text
week
month
quarter
```

Example:

```python
PrefixProbeConfig(
    cutpoint_mode=CutpointSelectionMode.PERIOD_END,
    period="month",
    min_prefix_rows=60,
)
```

This mode is useful when research workflows rebalance or retrain on calendar boundaries.

#### 6.2.3 `EXPLICIT`

`EXPLICIT` uses user-provided cutpoints:

```python
PrefixProbeConfig(
    cutpoint_mode=CutpointSelectionMode.EXPLICIT,
    explicit_cutpoints=(
        "2026-03-31T07:00:00+00:00",
        "2026-06-30T07:00:00+00:00",
    ),
)
```

Each explicit cutpoint must exist in the input `as_of` set after normal timestamp formatting. Missing explicit cutpoints should produce a probe warning metric, not silently shift to the nearest timestamp.

### 6.3 Prefix Recompute

For each cutpoint:

```text
prefix_input = input_frame.filter(pl.col("as_of") <= cutpoint)
prefix_factor_frame = runner.run(prefix_input, config)
```

Compare rows where:

```text
as_of <= cutpoint
```

The comparison window is selected by `compare_window_mode`.

#### 6.3.1 `TAIL_BARS`

`TAIL_BARS` compares only the last `compare_tail_bars` rows per symbol before each cutpoint.

This still catches common leakage patterns:

| Leakage pattern | Why tail compare catches it |
|---|---|
| `shift(-1)` | The last prefix row differs. |
| `shift(-N)` | The last N prefix rows differ. |
| centered rolling | Rows near the cutpoint differ. |
| full-sample scaling | Tail rows differ when future rows change global mean/std/max. |

Recommended rule:

```text
compare_tail_bars should be >= the suspected future dependency horizon.
```

Examples:

| Suspected leakage | Minimum useful `compare_tail_bars` |
|---|---|
| `shift(-1)` | 1 |
| `shift(-5)` | 5 |
| forward 20-bar label | 20 |
| centered window size 20 | 10 |
| unknown horizon | daily 30, minute 120 |

#### 6.3.2 `ALL_HISTORY`

If a project wants maximum sensitivity, set:

```python
PrefixProbeConfig(
    compare_window_mode=CompareWindowMode.ALL_HISTORY,
    compare_tail_bars=0,
)
```

Meaning:

```text
compare all rows <= cutpoint
```

This mode is slower, but it is the best option for deep audits and for detecting full-sample state such as global normalization.

MVP-1 should treat `compare_tail_bars = 0` as an alias for `ALL_HISTORY` for backward compatibility with simple configs.

### 6.4 Key Alignment

Comparison key:

```text
dataset_id
symbol
freq
as_of
factor_id
output_field
```

In wide Polars output, each `output_field` becomes a separate comparison series. The detector should normalize wide factor outputs into long comparison rows internally, matching FeatureStore semantics.

Missing keys are violations:

```text
prefix has key, full missing -> violation
full has key, prefix missing -> violation
```

The second case catches factor logic that only emits a historical row when future rows exist.

### 6.5 Value Comparison

Value comparison rules:

| Type | Rule |
|---|---|
| null vs null | Equal when `nulls_equal=True`. |
| NaN vs NaN | Equal when `nulls_equal=True`. |
| float | `abs(a - b) <= atol + rtol * abs(b)`. |
| int | Exact equality, or numeric tolerance after cast. |
| bool | Exact equality. |
| string | Exact equality. |

For MVP-1, all factor outputs are expected to be numeric or null. Non-numeric outputs may be compared exactly.

## 7. Quality Metric Integration

The detector should produce metrics compatible with `factor_quality_metric`.

New metric names:

```text
prefix_invariance_violation_count
prefix_probe_cutpoint_count
prefix_probe_compared_value_count
prefix_probe_changed_ratio
```

Severity:

```text
prefix_invariance_violation_count > 0 -> ERROR
prefix_probe_changed_ratio > 0         -> ERROR
all other probe metrics                -> INFO
```

`metric_json` for `prefix_invariance_violation_count`:

```json
{
  "check_level": "prefix_recompute",
  "as_of_semantics": "factor_value_timestamp",
  "cutpoint_mode": "evenly_spaced",
  "compare_window_mode": "tail_bars",
  "compare_tail_bars": 30,
  "cutpoints": [
    "2026-07-03T07:00:00+00:00"
  ],
  "examples": [
    {
      "cutpoint": "2026-07-03T07:00:00+00:00",
      "dataset_id": "fixture-daily",
      "symbol": "000001.SZ",
      "freq": "1d",
      "as_of": "2026-07-03T07:00:00+00:00",
      "factor_id": "forward_ret_1",
      "output_field": "forward_ret_1",
      "prefix_value": null,
      "full_value": 0.0123
    }
  ]
}
```

Status rule:

```text
any prefix_invariance_violation_count ERROR
  -> quality_status = FAILED
```

Do not overload the existing metadata-level `future_leakage_count` in MVP-1. Keep both:

```text
future_leakage_count                  # metadata / declared forward semantics
prefix_invariance_violation_count     # dynamic recompute evidence
```

Later, reports can include a derived field:

```text
leakage_detected = future_leakage_count > 0 OR prefix_invariance_violation_count > 0
```

## 8. Pipeline Placement

The detector belongs in the research pipeline after factor computation inputs are available and before downstream consumption.

Recommended sequence:

```text
read curated bars
  -> build factor input frame
  -> run full factor computation
  -> commit FeatureStore rows
  -> run FactorQualityAnalyzer on FeatureValue rows
  -> run PrefixInvarianceLeakageDetector on input frame
  -> merge quality metrics
  -> commit_quality_report(combined_report)
```

Why after commit is acceptable:

- FeatureStore `status=COMMITTED` means the rows were durably written.
- `quality_status=FAILED` means downstream consumers must not use them by default.
- Keeping failed-quality rows supports debugging and audit.

Strict production-style pipeline can later choose:

```text
run prefix detector before commit
if violation -> skip feature commit or commit only manifest
```

But MVP-1 should preserve the post-commit audit model already used by factor quality checks.

## 9. Examples

### 9.1 Causal Return

Input:

```python
op.pct_change(field("close"), periods=1)
```

Expected:

```text
prefix_invariance_violation_count = 0
quality_status remains PASSED if other checks pass
```

Reason:

```text
ret_1 at as_of=t only depends on close[t] and close[t-1]
```

### 9.2 Forward Return

Input:

```python
pl.col("close").shift(-1) / pl.col("close") - 1.0
```

At cutpoint `t`:

```text
prefix computation cannot know close[t+1]
full computation can know close[t+1]
```

Expected:

```text
prefix_invariance_violation_count > 0
severity = ERROR
quality_status = FAILED
```

### 9.3 Full-Sample Normalization

Input:

```python
(pl.col("close") - pl.col("close").mean()) / pl.col("close").std()
```

At cutpoint `t`:

```text
prefix mean/std != full mean/std
```

Expected:

```text
prefix_invariance_violation_count > 0
```

This is not a label, but it is still future leakage for a walk-forward research workflow.

## 10. False Positives and False Negatives

False positives can happen when:

- Input data has revisions after the fact.
- Adjusted prices are recomputed with future corporate actions.
- Factor computation is nondeterministic.
- Ranking universe differs between prefix and full runs because symbol membership is not versioned.

False negatives can happen when:

- Cutpoints miss the leaking window.
- Future rows happen not to change the computed value.
- Leakage depends on external data not included in the input frame.
- `as_of` is mis-modeled and does not represent the value timestamp.

Mitigations:

- Use deterministic cutpoints spread across the sample.
- Compare at least `max_forward_horizon` tail bars when known.
- Store examples for inspection.
- Keep static metadata checks alongside prefix recompute checks.

## 11. Test Plan

Minimum tests:

| Test | Expected |
|---|---|
| Causal `pct_change` factor has no prefix violations | report violation count is 0. |
| Native Polars `shift(-1)` factor is detected | violation count > 0 and examples retained. |
| Full-sample mean normalization is detected | violation count > 0. |
| Null/null comparison is stable | no violation for equal missing values. |
| Multi-symbol prefix compare does not mix symbols | keys include `symbol`. |
| Not enough cutpoints produces skipped INFO metrics | no ERROR when probe cannot run. |
| `EVENLY_SPACED` cutpoint mode is deterministic | repeated probes choose identical cutpoints. |
| `EXPLICIT` cutpoint mode rejects missing timestamps with warning metric | missing cutpoint is visible in report. |
| `ALL_HISTORY` compare mode compares all rows before cutpoint | compared row count exceeds tail-window mode. |
| Detector metrics can be merged into `FactorQualityReport` | `quality_status=FAILED` when violation exists. |

Suggested fixture:

```text
symbol = 000001.SZ
close = [10.0, 11.0, 12.0, 13.0, 14.0]
```

For `shift(-1) / close - 1`, a cutpoint at the third row should produce:

```text
prefix value at third row = null
full value at third row = 13.0 / 12.0 - 1
```

## 12. Open Implementation Decisions

These are intentionally deferred to the implementation plan:

1. Whether detector output should be its own report type or directly emit `FactorQualityMetric`.
2. Whether `FactorQualityReport` should gain a `merge(...)` helper.
3. Whether `PolarsFactorRunner.run(...)` should expose resolved factors to avoid resolving twice.
4. Whether prefix recompute should cache collected full results for speed.
5. Whether a failed prefix probe due to compute errors should mark quality `FAILED` or produce a probe error metric.

Recommended MVP choice:

```text
Detector returns a PrefixLeakageReport.
A small adapter converts PrefixLeakageReport into FactorQualityMetric rows.
FactorQualityReport gains a merge/additional_metrics helper.
```
