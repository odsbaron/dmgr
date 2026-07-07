# Factor Quality Checks Development Guide

> Branch: `feature/factor-quality-metrics`
> Scope: `feature_table` / `FeatureValue` -> `FactorQualityReport` -> `factor_quality_metric` -> manifest quality fields.
> Status: current implementation guide.

## 1. Purpose

因子质量检查层用于回答一个很具体的问题：

```text
这次 factor_run 产出的 feature_table，能不能被下游研究、训练、回放或准实盘模块消费？
```

当前实现不是重新计算因子，也不修复坏值，而是在 FeatureStore 已经把因子结果落成 `FeatureValue` 后，对每个 `factor_id + output_field` 生成结构化质量指标，并把结果写回 DuckDB。

当前链路：

```text
PolarsFactorRunner
  -> factor result LazyFrame
  -> LocalDuckDBFeatureStore.commit_run(...)
  -> feature_table / feature_snapshot / factor_run_manifest
  -> FactorQualityAnalyzer.analyze(feature_values, resolved_factors)
  -> LocalDuckDBFeatureStore.commit_quality_report(report)
  -> factor_quality_metric
  -> factor_run_manifest.quality_status / quality_summary_json
```

重要边界：

- `commit_run(...)` 负责写 feature 数据。
- `FactorQualityAnalyzer` 负责计算质量指标。
- `commit_quality_report(...)` 负责持久化质量报告并更新 manifest。
- 当前质量检查在 commit 后执行；它会标记 run 的 `quality_status`，但不会回滚已写入的 feature rows。
- 下游消费方必须读取 `factor_run_manifest.quality_status`，只消费 `PASSED` 或明确允许的 `WARNING` run。

## 2. Module Ownership

| Module | Responsibility | Non-responsibility |
|---|---|---|
| `quant_research.features.quality` | 指标计算、状态归并、质量报告对象 | 不连接 DuckDB，不知道表结构 |
| `quant_research.features.duckdb_store` | 写入 `factor_quality_metric`，更新 manifest | 不计算指标语义 |
| `quant_research.features.contracts` | manifest、feature value、snapshot 合约 | 不内置质量规则 |
| `quant_research.factors.contracts.FactorSpec` | 声明 `quality_rules` | 不执行质量检查 |
| pipeline/service layer | 串联 commit 和 quality report | 不直接拼 SQL 计算质量 |

这样拆的目的：以后可以替换质量分析实现，例如 Polars 批量质量分析、DuckDB SQL 质量分析、streaming incremental 质量分析，而 FeatureStore 的写入接口不需要变化。

## 3. Runtime Contracts

### 3.1 `QualitySeverity`

```text
INFO
WARNING
ERROR
```

当前实现只产生 `INFO` 和 `ERROR`。`WARNING` 作为合约预留，用于后续覆盖率偏低、极端值偏多、Python UDF 等非硬失败问题。

### 3.2 `QualityStatus`

```text
NOT_RUN
PASSED
WARNING
FAILED
```

含义：

| Status | Meaning |
|---|---|
| `NOT_RUN` | manifest 初始状态，尚未执行质量检查。 |
| `PASSED` | 没有任何 `ERROR` 或 `WARNING` 指标。 |
| `WARNING` | 至少一个 `WARNING`，没有 `ERROR`。当前实现暂不产生。 |
| `FAILED` | 至少一个 `ERROR`，或输入 feature values 为空。 |

状态归并逻辑：

```text
any metric.severity == ERROR   -> FAILED
else any severity == WARNING   -> WARNING
else                           -> PASSED
```

### 3.3 `FactorQualityMetric`

一条 metric 对应一个 `factor_run_id + factor_id + output_field + metric_name`。

| Field | Type | Meaning |
|---|---|---|
| `factor_run_id` | str | 本次因子运行 id。 |
| `feature_set_id` | str | 逻辑 feature set。 |
| `factor_id` | str | 因子 id。 |
| `output_field` | str | 因子输出字段。 |
| `metric_name` | str | 指标名，例如 `null_ratio`。 |
| `metric_value` | float | 指标数值。计数类也统一存 float，便于 DuckDB 查询。 |
| `metric_json` | dict | 指标明细，例如阈值、分子、规则来源。 |
| `severity` | `QualitySeverity` | `INFO` / `WARNING` / `ERROR`。 |
| `created_at` | str | ISO timestamp。 |

### 3.4 `FactorQualityReport`

一个 report 对应一次 factor run 的质量检查结果。

| Field | Type | Meaning |
|---|---|---|
| `factor_run_id` | str | 从第一条 `FeatureValue` 继承。 |
| `feature_set_id` | str | 从第一条 `FeatureValue` 继承。 |
| `status` | `QualityStatus` | 从 metric severity 归并。 |
| `metrics` | tuple[`FactorQualityMetric`, ...] | 所有 factor output 的指标。 |

`summary` 会生成 manifest 使用的精简 JSON：

```json
{
  "status": "FAILED",
  "metric_count": 10,
  "severity_counts": {
    "ERROR": 1,
    "INFO": 9
  }
}
```

## 4. DuckDB Persistence

### 4.1 `factor_quality_metric`

当前建表字段：

| Column | Type | Rule |
|---|---|---|
| `factor_run_id` | VARCHAR | 必填。 |
| `feature_set_id` | VARCHAR | 必填。 |
| `factor_id` | VARCHAR | 必填。 |
| `output_field` | VARCHAR | 必填。 |
| `metric_name` | VARCHAR | 必填。 |
| `metric_value` | DOUBLE | 必填。 |
| `metric_json` | VARCHAR | JSON string，必填。 |
| `severity` | VARCHAR | `INFO` / `WARNING` / `ERROR`。 |
| `created_at` | VARCHAR | ISO timestamp。 |

当前没有数据库唯一约束；幂等性由 `commit_quality_report(...)` 在写入前删除同一 `factor_run_id` 的旧指标保证：

```sql
DELETE FROM factor_quality_metric WHERE factor_run_id = ?
```

然后重新插入本次 report 的全部 metrics。

### 4.2 `factor_run_manifest` quality fields

新增质量字段：

| Column | Type | Meaning |
|---|---|---|
| `quality_status` | VARCHAR | `NOT_RUN` / `PASSED` / `WARNING` / `FAILED`。 |
| `quality_summary_json` | VARCHAR | `FactorQualityReport.summary` 的 JSON string。 |

新建 manifest 时默认：

```text
quality_status = "NOT_RUN"
quality_summary_json = "{}"
```

写入质量报告后：

```sql
UPDATE factor_run_manifest
SET quality_status = ?, quality_summary_json = ?
WHERE factor_run_id = ?
```

当前注意事项：

- `commit_quality_report(...)` 假设对应 manifest 已存在。
- 如果未来支持独立质量任务，需要对不存在的 manifest 返回明确错误。
- 已有旧 DuckDB 文件如果缺少新增列，需要迁移脚本；当前测试使用新库。

## 5. Metrics

当前每个 `factor_id + output_field` 输出 5 个指标：

```text
row_count
null_ratio
warmup_incomplete_count
duplicate_key_count
future_leakage_count
```

### 5.1 `row_count`

定义：

```text
row_count = count(values scoped by factor_id and output_field)
```

severity：

```text
always INFO
```

用途：

- 作为其他比例指标的分母。
- 快速发现某个 declared output 没有落出任何 feature row。
- 后续可以衍生为 `missing_output_count` 或 `coverage_ratio`。

当前边界：

- 当某个 output 没有任何值时，`row_count = 0`。
- `FactorQualityAnalyzer.analyze([])` 会直接返回 `FAILED` 且 metrics 为空。

### 5.2 `null_ratio`

定义：

```text
null_count = count(value.value_kind == "null")
null_ratio = null_count / row_count if row_count > 0 else 1.0
```

阈值来源：

```python
FactorSpec(
    quality_rules={
        "max_null_ratio": 0.5
    }
)
```

默认值：

```text
max_null_ratio = 1.0
```

severity：

```text
null_ratio > max_null_ratio -> ERROR
otherwise                   -> INFO
```

注意是严格大于：

```text
null_ratio == max_null_ratio -> INFO
```

metric_json：

```json
{
  "null_count": 2,
  "max_null_ratio": 0.5
}
```

设计理由：

- warmup 产生的 null 也计入 `null_ratio`。
- 是否允许 warmup null 由 `max_null_ratio` 控制。
- 后续如果希望区分 warmup null 和异常 null，可以新增 `post_warmup_null_ratio`。

### 5.3 `warmup_incomplete_count`

定义：

```text
warmup_incomplete_count = count(value.warmup_complete == false)
```

severity：

```text
always INFO
```

当前语义：

- 它是观测指标，不是硬失败。
- 用于解释 `null_ratio`、回测起始阶段样本损失、snapshot 是否可消费。

为什么当前不拦截：

- rolling 类因子天然存在 warmup 区间。
- 第一版希望保留完整 feature_table，交给 snapshot 或下游训练窗口过滤。
- 如果某些训练任务要求完全 warmup，可以在 pipeline 层加 `warmup_incomplete_count == 0` 的消费规则。

后续可扩展：

```text
quality_rules.require_warmup_complete = true
  -> warmup_incomplete_count > 0 -> ERROR
```

### 5.4 `duplicate_key_count`

定义的 feature key：

```text
feature_set_id
dataset_id
symbol
freq
as_of
factor_id
factor_version
output_field
```

计算：

```text
对于每个 key 分组:
  如果 count > 1，则重复数贡献 count - 1

duplicate_key_count = sum(count - 1 for duplicate groups)
```

severity：

```text
duplicate_key_count > 0 -> ERROR
otherwise               -> INFO
```

为什么是 ERROR：

- 重复 feature key 会让 snapshot、训练矩阵、回放输入出现不可确定行为。
- 同一个 `as_of + factor_id + output_field` 只能有一个值。

当前补充：

- FeatureStore 的 commit 阶段已经有 duplicate feature key 防线。
- 质量层继续计算该指标，是为了在未来支持外部导入 feature_table 或跨 run merge 时仍能复用检查。

### 5.5 `future_leakage_count`

当前采用“前向计算元数据判定”，不是行级表达式追踪。

触发规则：

```text
forward_bars > 0 OR uses_future_data = true OR causal = false
  -> future_leakage_count = row_count
otherwise
  -> future_leakage_count = 0
```

对应 FactorSpec：

```python
FactorSpec(
    factor_id="forward_ret_1",
    quality_rules={
        "forward_bars": 1,
        "causal": False,
    },
)
```

另一种显式写法：

```python
FactorSpec(
    factor_id="label_next_return",
    quality_rules={
        "uses_future_data": True,
        "causal": False,
    },
)
```

severity：

```text
future_leakage_count > 0 -> ERROR
otherwise                -> INFO
```

metric_json：

```json
{
  "check_level": "forward_metadata",
  "forward_bars": 1,
  "uses_future_data": false,
  "causal": false
}
```

为什么这样设计：

- 当前因子 runner 尚未记录每个输出字段的 `input_window_start` / `input_window_end`。
- 对 forward return、未来收益 label、未来最大回撤等目标变量，最安全的第一版规则是：只要声明使用未来数据，就不能伪装成普通可交易特征。
- `future_leakage_count = row_count` 表示这个输出字段的每一行都属于 forward/label 语义，不应该直接进入准实盘 feature snapshot。

重要区分：

```text
可交易特征: as_of 时刻或之前可获得的数据计算出来的值
训练标签: as_of 之后的收益、回撤、方向、成交结果
```

训练标签可以存在，但应该进入 label store 或 training dataset 的 label side，不应该混入 live feature store。

未来行级增强：

```text
input_window_end > as_of -> future_leakage_count += 1
```

行级 lineage 落地后，`check_level` 可以从 `forward_metadata` 升级为：

```text
row_lineage
operator_lineage
expression_lineage
```

## 6. Call Pattern

当前推荐调用：

```python
store = LocalDuckDBFeatureStore("data/research.duckdb")

commit = store.commit_run(request)
if commit.status == FeatureRunStatus.COMMITTED:
    values = store.read_feature_table(commit.feature_table_ref)
    report = FactorQualityAnalyzer().analyze(values, request.resolved_factors)
    store.commit_quality_report(report)
```

读取结果：

```python
metrics = store.list_quality_metrics("factor-run-1")
manifest = store.get_manifest("factor-run-1")

assert manifest.quality_status in {"PASSED", "WARNING", "FAILED"}
```

下游消费建议：

```text
Research notebook:
  allow PASSED
  optionally allow WARNING
  block FAILED unless explicitly debugging

Training dataset:
  require PASSED for feature side
  labels must come from a separate label side

Backtest / replay:
  require PASSED by default

Quasi-live:
  require PASSED
```

## 7. SQL Inspection

查看某次 run 的 manifest 状态：

```sql
SELECT
  factor_run_id,
  feature_set_id,
  dataset_id,
  freq,
  status,
  quality_status,
  quality_summary_json
FROM factor_run_manifest
WHERE factor_run_id = 'factor-run-1';
```

查看错误指标：

```sql
SELECT
  factor_id,
  output_field,
  metric_name,
  metric_value,
  metric_json,
  severity
FROM factor_quality_metric
WHERE factor_run_id = 'factor-run-1'
  AND severity = 'ERROR'
ORDER BY factor_id, output_field, metric_name;
```

查看前向泄露检查：

```sql
SELECT
  factor_id,
  output_field,
  metric_value AS future_leakage_count,
  metric_json
FROM factor_quality_metric
WHERE factor_run_id = 'factor-run-1'
  AND metric_name = 'future_leakage_count';
```

查看 null ratio 排名：

```sql
SELECT
  factor_id,
  output_field,
  metric_value AS null_ratio,
  metric_json,
  severity
FROM factor_quality_metric
WHERE factor_run_id = 'factor-run-1'
  AND metric_name = 'null_ratio'
ORDER BY null_ratio DESC;
```

## 8. Acceptance Coverage

当前测试覆盖：

| Test | Coverage |
|---|---|
| `test_quality_analyzer_computes_null_ratio_and_warmup_count` | `row_count`、`null_ratio`、`warmup_incomplete_count` 正确计算。 |
| `test_quality_analyzer_marks_null_ratio_over_threshold_as_error` | 超过 `max_null_ratio` 产生 `ERROR`，report 为 `FAILED`。 |
| `test_quality_analyzer_counts_forward_bars_as_future_leakage_error` | `forward_bars > 0` 产生 `future_leakage_count` ERROR。 |
| `test_quality_analyzer_counts_non_causal_factor_as_future_leakage_error` | `causal=False` 产生 `future_leakage_count` ERROR。 |
| `test_quality_analyzer_detects_duplicate_feature_keys` | 重复 feature key 产生 `duplicate_key_count` ERROR。 |
| `test_feature_store_writes_quality_metrics_and_updates_manifest` | quality report 写入 `factor_quality_metric`，manifest 更新为 `PASSED`。 |
| `test_feature_store_marks_manifest_quality_failed_for_forward_leakage` | 前向泄露 report 写入后，manifest 更新为 `FAILED`。 |

验证命令：

```bash
.venv/bin/python -m pytest -v tests/features/test_factor_quality.py tests/features/test_duckdb_feature_store.py
.venv/bin/python -m pytest -v
.venv/bin/ruff check src tests
git diff --check
```

## 9. Design Decisions

### 9.1 为什么质量检查放在 FeatureStore 之后

质量检查的输入应该是已经标准化的 `FeatureValue`，而不是宽表 LazyFrame。

原因：

- `FeatureValue` 已经带齐 `factor_run_id`、`feature_set_id`、`factor_version`、`warmup_complete`、`value_kind`。
- 长表天然适合逐 output 做质量指标。
- 后续 DuckDB SQL、Polars、streaming incremental 都可以复用同一个合约。

### 9.2 为什么前向泄露先用元数据判定

表达式级 future leakage 检查需要完整 lineage：

```text
operator input window
rolling window direction
shift direction
label horizon
as_of alignment
market data availability time
```

当前系统还没有这些字段。先用 `quality_rules` 显式声明 forward/causal 语义，能以最低复杂度阻止 label 混入 feature。

### 9.3 为什么 quality status 不直接覆盖 run status

`status` 和 `quality_status` 表达的是两个不同层面：

| Field | Meaning |
|---|---|
| `status` | 这次数据写入是否成功。 |
| `quality_status` | 写入的数据是否适合被下游消费。 |

因此一个 run 可以是：

```text
status = COMMITTED
quality_status = FAILED
```

这表示数据成功落库，但质量门不允许下游默认消费。

## 10. Known Gaps

当前尚未实现：

1. `symbol_count`、`as_of_min`、`as_of_max`、`coverage_ratio`。
2. `post_warmup_null_ratio`，用于区分 warmup null 和异常 null。
3. `extreme_value_count`，用于检测离群值或无穷值。
4. 行级 `input_window_start` / `input_window_end` lineage。
5. 质量检查在 commit 前阻断写入的 strict pipeline。
6. 旧 DuckDB schema migration。
7. `factor_quality_metric` 唯一索引或主键。
8. 对不存在 manifest 的 `commit_quality_report` 显式失败。

建议下一步顺序：

```text
1. FactorRunService 串联 runner -> feature store -> quality analyzer
2. 下游读取 feature_snapshot 前强制检查 quality_status
3. 新增 coverage / post_warmup_null_ratio / extreme_value_count
4. 新增 label store，承接 forward calculation 输出
5. 增加 row-level lineage 后升级 future leakage 检查
```

## 11. Minimal Example

普通可交易因子：

```python
FactorSpec(
    factor_id="ret_1",
    version="1.0.0",
    namespace="price",
    description="One bar close-to-close return.",
    input_fields=("close",),
    output_fields=("ret_1",),
    supported_freqs=(Frequency.D1,),
    lookback_bars=2,
    warmup_bars=1,
    compute_mode=ComputeMode.OPERATOR_GRAPH,
    quality_rules={
        "max_null_ratio": 0.1,
        "forward_bars": 0,
        "causal": True,
    },
)
```

训练标签或前向收益：

```python
FactorSpec(
    factor_id="forward_ret_1",
    version="1.0.0",
    namespace="label",
    description="Next bar return label.",
    input_fields=("close",),
    output_fields=("forward_ret_1",),
    supported_freqs=(Frequency.D1,),
    lookback_bars=1,
    warmup_bars=0,
    compute_mode=ComputeMode.OPERATOR_GRAPH,
    quality_rules={
        "forward_bars": 1,
        "causal": False,
    },
)
```

预期：

```text
ret_1:
  future_leakage_count = 0
  severity = INFO

forward_ret_1:
  future_leakage_count = row_count
  severity = ERROR
  manifest.quality_status = FAILED
```

