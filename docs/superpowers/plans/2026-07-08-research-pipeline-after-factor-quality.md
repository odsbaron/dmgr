# Research Pipeline After Factor Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next development slice after factor quality checks: an end-to-end batch research pipeline that reads curated bars, computes factors, commits features, writes quality reports, gates downstream consumption, and prepares the path for labels and training matrices.

**Architecture:** Keep the system decoupled by adding a thin orchestration layer instead of merging data, factor, feature, and quality code. The pipeline coordinates existing ports: `LocalDuckDBStore` for bars, `PolarsFactorRunner` for factor computation, `LocalDuckDBFeatureStore` for durable features, and `FactorQualityAnalyzer` for quality reports. Forward-looking outputs stay out of live feature consumption and later move into a separate label store.

**Tech Stack:** Python 3.14, Polars LazyFrame, DuckDB, pytest, ruff, local `DataRef` contracts.

---

## Current State

Already implemented:

- Data ingestion from CSV K lines into DuckDB `curated_market_bar`.
- `DataRef` contract for DuckDB table slices.
- `FactorSpec`, `FactorRegistry`, Operator DSL-lite, native Polars factor modes.
- `PolarsFactorRunner`.
- `FeatureStore` write path: `feature_table`, `feature_snapshot`, `factor_run_manifest`.
- `FactorQualityAnalyzer` with:
  - `row_count`
  - `null_ratio`
  - `warmup_incomplete_count`
  - `duplicate_key_count`
  - `future_leakage_count`
- `factor_quality_metric` persistence and manifest `quality_status`.

Next architectural target:

```text
DataRef(curated_market_bar)
  -> bar frame adapter
  -> PolarsFactorRunner
  -> FeatureStore.commit_run
  -> FactorQualityAnalyzer
  -> FeatureStore.commit_quality_report
  -> quality-gated feature_snapshot ref
  -> later: feature matrix and label store
```

## File Structure

Create:

- `src/quant_research/pipeline/__init__.py`
  - Package marker for orchestration-level code.
- `src/quant_research/pipeline/bar_frame.py`
  - Convert `BarRecord` objects from the data layer into Polars `LazyFrame` input for factor computation.
- `src/quant_research/pipeline/contracts.py`
  - Pipeline request/result/status contracts.
- `src/quant_research/pipeline/research.py`
  - End-to-end orchestration service.
- `tests/pipeline/test_bar_frame.py`
  - Unit tests for bar-to-factor-frame conversion.
- `tests/pipeline/test_research_pipeline.py`
  - End-to-end pipeline tests with local DuckDB stores.
- `src/quant_research/features/gates.py`
  - Consumer-side quality gate helper for feature snapshot usage.
- `tests/features/test_feature_quality_gate.py`
  - Tests for blocking failed quality runs before downstream consumption.
- `src/quant_research/labels/__init__.py`
  - Package marker for labels.
- `src/quant_research/labels/contracts.py`
  - Label value, label commit request, label manifest.
- `src/quant_research/labels/duckdb_store.py`
  - DuckDB label storage.
- `tests/labels/test_duckdb_label_store.py`
  - Label store tests.
- `src/quant_research/datasets/__init__.py`
  - Package marker for training dataset builders.
- `src/quant_research/datasets/feature_matrix.py`
  - Build model-ready Polars frames from quality-gated features and optional labels.
- `tests/datasets/test_feature_matrix.py`
  - Feature matrix tests.
- `docs/development/research-pipeline-development.md`
  - Architecture and usage documentation for the new pipeline.

Modify:

- `README.md`
  - Add pipeline and later label/matrix docs to development docs and entry points.
- `docs/development/factor-quality-checks.md`
  - Add a section explaining how the pipeline consumes `quality_status`.
- `docs/development/feature-store-spec.md`
  - Add a short reference to the consumer-side quality gate.

Do not modify in this slice:

- Data ingestion internals.
- Factor DSL internals.
- Existing FeatureStore row layout, except for small quality metrics additions in Task 6.
- Trading runtime modules such as order, account, gateway, risk.

## Milestone Order

1. M1: End-to-end research pipeline.
2. M2: Consumer-side quality gate.
3. M3: Additional quality metrics.
4. M4: Label store for forward/target outputs.
5. M5: Feature matrix builder.
6. M6: Docs and example.

Each milestone should land as a separate commit. If work is split across branches, branch from `feature/factor-quality-metrics` after it is rebased or merged as needed.

---

### Task 1: Bar Frame Adapter

**Files:**
- Create: `src/quant_research/pipeline/__init__.py`
- Create: `src/quant_research/pipeline/bar_frame.py`
- Create: `tests/pipeline/test_bar_frame.py`

- [ ] **Step 1: Write the failing test**

Create `tests/pipeline/test_bar_frame.py`:

```python
from datetime import UTC, date, datetime

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.pipeline.bar_frame import bars_to_factor_frame


def bar(symbol: str, close: str, index: int) -> BarRecord:
    start = datetime(2026, 7, 1 + index, 1, 30, tzinfo=UTC)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.STOCK,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=start,
        bar_end_time=start,
        open="10.0",
        high="10.5",
        low="9.9",
        close=close,
        volume="1000",
        turnover="10000",
        adjustment=Adjustment.NONE,
        source="csv",
        source_run_id="import-run-1",
        source_row_id=f"row-{index}",
        raw_ref="fixture.csv",
    )


def test_bars_to_factor_frame_preserves_keys_and_casts_numeric_values():
    frame = bars_to_factor_frame(
        [
            bar("000001.SZ", "10.1", 0),
            bar("000001.SZ", "10.3", 1),
        ]
    ).collect()

    assert frame.columns == [
        "dataset_id",
        "symbol",
        "exchange",
        "asset_class",
        "freq",
        "trading_date",
        "as_of",
        "bar_start_time",
        "bar_end_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "adjustment",
        "source_run_id",
    ]
    assert frame["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert frame["freq"].to_list() == ["1d", "1d"]
    assert frame["close"].to_list() == [10.1, 10.3]
    assert frame["as_of"].to_list()[0].isoformat() == "2026-07-01T01:30:00+00:00"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_bar_frame.py
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.pipeline'
```

- [ ] **Step 3: Implement the adapter**

Create `src/quant_research/pipeline/__init__.py`:

```python
"""Research pipeline orchestration helpers."""
```

Create `src/quant_research/pipeline/bar_frame.py`:

```python
from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from quant_research.contracts.bar import BarRecord


def bars_to_factor_frame(bars: Iterable[BarRecord]) -> pl.LazyFrame:
    rows = [
        {
            "dataset_id": bar.dataset_id,
            "symbol": bar.symbol,
            "exchange": bar.exchange,
            "asset_class": bar.asset_class.value,
            "freq": bar.freq.value,
            "trading_date": bar.trading_date.isoformat(),
            "as_of": bar.bar_end_time,
            "bar_start_time": bar.bar_start_time,
            "bar_end_time": bar.bar_end_time,
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "volume": float(bar.volume),
            "turnover": float(bar.turnover) if bar.turnover is not None else None,
            "adjustment": bar.adjustment.value,
            "source_run_id": bar.source_run_id,
        }
        for bar in bars
    ]
    return pl.DataFrame(rows).lazy()
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_bar_frame.py
```

Expected:

```text
1 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/quant_research/pipeline/__init__.py src/quant_research/pipeline/bar_frame.py tests/pipeline/test_bar_frame.py
git commit -m "feat: add bar frame adapter"
```

---

### Task 2: Pipeline Contracts

**Files:**
- Create: `src/quant_research/pipeline/contracts.py`
- Create: `tests/pipeline/test_research_pipeline.py`

- [ ] **Step 1: Write the failing contract test**

Create `tests/pipeline/test_research_pipeline.py` with the first test:

```python
from quant_research.contracts.bar import Frequency
from quant_research.contracts.refs import DataRef
from quant_research.factors.contracts import FactorRunConfig
from quant_research.pipeline.contracts import (
    ResearchPipelineRequest,
    ResearchPipelineStatus,
)


def run_config(*factor_ids: str) -> FactorRunConfig:
    return FactorRunConfig(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d&adjustment=NONE&source_run_id=import-run-1",
        factor_ids=factor_ids,
        freq=Frequency.D1,
        dataset_id="fixture-daily",
    )


def test_research_pipeline_request_uses_config_input_ref_by_default():
    request = ResearchPipelineRequest(config=run_config("ret_1"))

    assert request.input_data_ref == DataRef.parse(request.config.input_data_ref)
    assert ResearchPipelineStatus.COMMITTED.value == "COMMITTED"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_research_pipeline.py::test_research_pipeline_request_uses_config_input_ref_by_default
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.pipeline.contracts'
```

- [ ] **Step 3: Implement contracts**

Create `src/quant_research/pipeline/contracts.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_research.contracts.refs import DataRef
from quant_research.factors.contracts import FactorRunConfig
from quant_research.features.contracts import FeatureCommitResult, FeatureRunManifest
from quant_research.features.quality import FactorQualityReport


class ResearchPipelineStatus(StrEnum):
    COMMITTED = "COMMITTED"
    QUALITY_FAILED = "QUALITY_FAILED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ResearchPipelineRequest:
    config: FactorRunConfig
    input_data_ref: DataRef | None = None

    def __post_init__(self) -> None:
        if self.input_data_ref is None:
            object.__setattr__(self, "input_data_ref", DataRef.parse(self.config.input_data_ref))


@dataclass(frozen=True)
class ResearchPipelineResult:
    factor_run_id: str
    status: ResearchPipelineStatus
    feature_commit: FeatureCommitResult | None
    quality_report: FactorQualityReport | None
    manifest: FeatureRunManifest | None
    error_code: str | None = None
    error_message: str | None = None

    @property
    def usable(self) -> bool:
        return self.status == ResearchPipelineStatus.COMMITTED
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_research_pipeline.py::test_research_pipeline_request_uses_config_input_ref_by_default
```

Expected:

```text
1 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/quant_research/pipeline/contracts.py tests/pipeline/test_research_pipeline.py
git commit -m "feat: add research pipeline contracts"
```

---

### Task 3: End-to-End Research Pipeline Happy Path

**Files:**
- Create: `src/quant_research/pipeline/research.py`
- Modify: `tests/pipeline/test_research_pipeline.py`

- [ ] **Step 1: Add a failing happy-path test**

Append to `tests/pipeline/test_research_pipeline.py`:

```python
from pathlib import Path

from quant_research.contracts.bar import Adjustment
from quant_research.contracts.import_run import ImportRun
from quant_research.contracts.quality import QualityReport
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.normalize import BarNormalizer
from quant_research.data.readers.csv_reader import CSVKLineReader
from quant_research.factors.contracts import ComputeMode, FactorSpec
from quant_research.factors.dsl import field, op
from quant_research.factors.registry import FactorRegistry
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.pipeline.research import ResearchPipelineService


def daily_source_spec(path: Path) -> SourceSpec:
    return SourceSpec(
        source_id="fixture_daily",
        dataset_id="fixture-daily",
        source_type=SourceType.CSV,
        path=str(path),
        freq=Frequency.D1,
        timezone="Asia/Shanghai",
        adjustment=Adjustment.NONE,
        field_mapping={
            "symbol": "symbol",
            "exchange": "exchange",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "turnover": "turnover",
        },
        symbol_mapping={},
        calendar_id="cn_stock_simple",
    )


def committed_bar_ref(tmp_path):
    store = LocalDuckDBStore(tmp_path / "research.duckdb")
    source_spec = daily_source_spec(Path("tests/fixtures/bars_daily.csv"))
    import_run = ImportRun.create(
        import_run_id="import-run-1",
        dataset_id="fixture-daily",
        source_id="fixture_daily",
        freq=Frequency.D1,
        adjustment=Adjustment.NONE,
        source_file_hash="sha256:fixture",
    )
    normalizer = BarNormalizer(import_run_id=import_run.import_run_id)
    bars = [normalizer.normalize(row, source_spec) for row in CSVKLineReader().read_rows(source_spec)]
    data_ref = store.commit_import(import_run, bars, QualityReport(import_run.import_run_id, ()))
    return store, data_ref


def registry_with_ret_1() -> FactorRegistry:
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="ret_1",
        version="1.0.0",
        namespace="price",
        description="One bar return.",
        input_fields=("close",),
        output_fields=("ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=2,
        warmup_bars=1,
        compute_mode=ComputeMode.OPERATOR_GRAPH,
        quality_rules={"max_null_ratio": 0.6, "forward_bars": 0, "causal": True},
    )
    registry.register(spec, op.pct_change(field("close"), periods=1).alias("ret_1"))
    return registry


def test_research_pipeline_commits_features_and_quality_report(tmp_path):
    data_store, data_ref = committed_bar_ref(tmp_path)
    feature_store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    registry = registry_with_ret_1()
    config = run_config("ret_1")
    request = ResearchPipelineRequest(config=config, input_data_ref=data_ref)

    result = ResearchPipelineService(
        data_store=data_store,
        feature_store=feature_store,
        factor_registry=registry,
    ).run(request)

    metrics = feature_store.list_quality_metrics("factor-run-1")
    manifest = feature_store.get_manifest("factor-run-1")

    assert result.status == ResearchPipelineStatus.COMMITTED
    assert result.usable is True
    assert result.feature_commit is not None
    assert result.feature_commit.row_count_feature == 2
    assert result.quality_report is not None
    assert manifest.quality_status == "PASSED"
    assert any(metric.metric_name == "null_ratio" for metric in metrics)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_research_pipeline.py::test_research_pipeline_commits_features_and_quality_report
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.pipeline.research'
```

- [ ] **Step 3: Implement `ResearchPipelineService`**

Create `src/quant_research/pipeline/research.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.factors.registry import FactorRegistry
from quant_research.features.contracts import FeatureCommitRequest, FeatureRunStatus
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.quality import FactorQualityAnalyzer, QualityStatus
from quant_research.pipeline.bar_frame import bars_to_factor_frame
from quant_research.pipeline.contracts import (
    ResearchPipelineRequest,
    ResearchPipelineResult,
    ResearchPipelineStatus,
)


@dataclass
class ResearchPipelineService:
    data_store: LocalDuckDBStore
    feature_store: LocalDuckDBFeatureStore
    factor_registry: FactorRegistry
    quality_analyzer: FactorQualityAnalyzer | None = None

    def __post_init__(self) -> None:
        if self.quality_analyzer is None:
            self.quality_analyzer = FactorQualityAnalyzer()

    def run(self, request: ResearchPipelineRequest) -> ResearchPipelineResult:
        try:
            bars = self.data_store.read_bars(request.input_data_ref)
            resolved = tuple(
                self.factor_registry.resolve_many(
                    request.config.factor_ids,
                    freq=request.config.freq,
                )
            )
            factor_frame = PolarsFactorRunner(self.factor_registry).run(
                bars_to_factor_frame(bars),
                request.config,
            )
            commit = self.feature_store.commit_run(
                FeatureCommitRequest(
                    config=request.config,
                    factor_frame=factor_frame,
                    resolved_factors=resolved,
                    input_row_count=len(bars),
                )
            )
            if commit.status != FeatureRunStatus.COMMITTED:
                manifest = self.feature_store.get_manifest(request.config.factor_run_id)
                return ResearchPipelineResult(
                    factor_run_id=request.config.factor_run_id,
                    status=ResearchPipelineStatus.FAILED,
                    feature_commit=commit,
                    quality_report=None,
                    manifest=manifest,
                    error_code=commit.error_code,
                    error_message=commit.error_message,
                )

            values = self.feature_store.read_feature_table(commit.feature_table_ref)
            quality_analyzer = self.quality_analyzer
            if quality_analyzer is None:
                raise RuntimeError("quality analyzer is not configured")
            report = quality_analyzer.analyze(values, resolved)
            self.feature_store.commit_quality_report(report)
            manifest = self.feature_store.get_manifest(request.config.factor_run_id)
            if request.config.strict_quality and report.status == QualityStatus.FAILED:
                return ResearchPipelineResult(
                    factor_run_id=request.config.factor_run_id,
                    status=ResearchPipelineStatus.QUALITY_FAILED,
                    feature_commit=commit,
                    quality_report=report,
                    manifest=manifest,
                    error_code="QUALITY_GATE_FAILED",
                    error_message="factor quality report contains ERROR metrics",
                )
            return ResearchPipelineResult(
                factor_run_id=request.config.factor_run_id,
                status=ResearchPipelineStatus.COMMITTED,
                feature_commit=commit,
                quality_report=report,
                manifest=manifest,
            )
        except Exception as exc:
            return ResearchPipelineResult(
                factor_run_id=request.config.factor_run_id,
                status=ResearchPipelineStatus.FAILED,
                feature_commit=None,
                quality_report=None,
                manifest=self.feature_store.get_manifest(request.config.factor_run_id),
                error_code="RESEARCH_PIPELINE_FAILED",
                error_message=str(exc),
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_research_pipeline.py::test_research_pipeline_commits_features_and_quality_report
```

Expected:

```text
1 passed
```

- [ ] **Step 5: Run pipeline tests**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline
```

Expected:

```text
3 passed
```

- [ ] **Step 6: Commit**

```bash
git add src/quant_research/pipeline/research.py tests/pipeline/test_research_pipeline.py
git commit -m "feat: add research pipeline service"
```

---

### Task 4: Strict Quality Gate for Forward Calculations

**Files:**
- Modify: `tests/pipeline/test_research_pipeline.py`
- Modify: `src/quant_research/pipeline/research.py`

- [ ] **Step 1: Add a failing strict-quality test**

Append to `tests/pipeline/test_research_pipeline.py`:

```python
def registry_with_forward_label() -> FactorRegistry:
    registry = FactorRegistry()
    spec = FactorSpec(
        factor_id="forward_ret_1",
        version="1.0.0",
        namespace="label",
        description="Next bar return label.",
        input_fields=("close",),
        output_fields=("forward_ret_1",),
        supported_freqs=(Frequency.D1,),
        lookback_bars=1,
        warmup_bars=0,
        compute_mode=ComputeMode.POLARS_EXPR,
        quality_rules={"forward_bars": 1, "causal": False},
    )
    registry.register(
        spec,
        lambda _spec, _config: [(pl.col("close").shift(-1) / pl.col("close") - 1.0).alias("forward_ret_1")],
    )
    return registry


def test_research_pipeline_marks_forward_label_run_quality_failed(tmp_path):
    data_store, data_ref = committed_bar_ref(tmp_path)
    feature_store = LocalDuckDBFeatureStore(tmp_path / "research.duckdb")
    config = FactorRunConfig(
        factor_run_id="factor-run-forward",
        feature_set_id="label_probe_v1",
        input_data_ref=data_ref.uri,
        factor_ids=("forward_ret_1",),
        freq=Frequency.D1,
        dataset_id="fixture-daily",
        strict_quality=True,
    )

    result = ResearchPipelineService(
        data_store=data_store,
        feature_store=feature_store,
        factor_registry=registry_with_forward_label(),
    ).run(ResearchPipelineRequest(config=config, input_data_ref=data_ref))

    manifest = feature_store.get_manifest("factor-run-forward")

    assert result.status == ResearchPipelineStatus.QUALITY_FAILED
    assert result.usable is False
    assert result.error_code == "QUALITY_GATE_FAILED"
    assert manifest.quality_status == "FAILED"
```

Add the missing Polars import near the top:

```python
import polars as pl
```

- [ ] **Step 2: Run test**

Run:

```bash
.venv/bin/python -m pytest -v tests/pipeline/test_research_pipeline.py::test_research_pipeline_marks_forward_label_run_quality_failed
```

Expected:

```text
1 passed
```

This test may already pass after Task 3. If it does, keep it as regression coverage and do not change production code.

- [ ] **Step 3: Commit**

```bash
git add tests/pipeline/test_research_pipeline.py src/quant_research/pipeline/research.py
git commit -m "test: cover strict factor quality gate"
```

---

### Task 5: Consumer-Side Feature Quality Gate

**Files:**
- Create: `src/quant_research/features/gates.py`
- Create: `tests/features/test_feature_quality_gate.py`

- [ ] **Step 1: Write failing gate tests**

Create `tests/features/test_feature_quality_gate.py`:

```python
import pytest

from quant_research.features.gates import FeatureQualityGateError, require_usable_manifest
from quant_research.features.quality import QualityStatus


class Manifest:
    def __init__(self, quality_status: str):
        self.factor_run_id = "factor-run-1"
        self.quality_status = quality_status


def test_require_usable_manifest_accepts_passed():
    manifest = Manifest(QualityStatus.PASSED.value)

    assert require_usable_manifest(manifest) is manifest


def test_require_usable_manifest_rejects_failed():
    manifest = Manifest(QualityStatus.FAILED.value)

    with pytest.raises(FeatureQualityGateError, match="factor-run-1"):
        require_usable_manifest(manifest)


def test_require_usable_manifest_can_accept_warning_when_allowed():
    manifest = Manifest(QualityStatus.WARNING.value)

    assert require_usable_manifest(manifest, allow_warning=True) is manifest
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/features/test_feature_quality_gate.py
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.features.gates'
```

- [ ] **Step 3: Implement gate helper**

Create `src/quant_research/features/gates.py`:

```python
from __future__ import annotations

from typing import Protocol, TypeVar

from quant_research.features.quality import QualityStatus


class FeatureManifestLike(Protocol):
    factor_run_id: str
    quality_status: str


class FeatureQualityGateError(ValueError):
    pass


TManifest = TypeVar("TManifest", bound=FeatureManifestLike)


def require_usable_manifest(
    manifest: TManifest,
    *,
    allow_warning: bool = False,
) -> TManifest:
    allowed = {QualityStatus.PASSED.value}
    if allow_warning:
        allowed.add(QualityStatus.WARNING.value)
    if manifest.quality_status not in allowed:
        raise FeatureQualityGateError(
            f"factor run {manifest.factor_run_id} is not usable: "
            f"quality_status={manifest.quality_status}"
        )
    return manifest
```

- [ ] **Step 4: Run tests**

Run:

```bash
.venv/bin/python -m pytest -v tests/features/test_feature_quality_gate.py
```

Expected:

```text
3 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/quant_research/features/gates.py tests/features/test_feature_quality_gate.py
git commit -m "feat: add feature quality consumption gate"
```

---

### Task 6: Add Quality Metric Extensions

**Files:**
- Modify: `src/quant_research/features/quality.py`
- Modify: `tests/features/test_factor_quality.py`
- Modify: `docs/development/factor-quality-checks.md`

Add three metrics now:

```text
symbol_count
post_warmup_null_ratio
as_of_span_days
```

Do not add coverage ratio until expected trading calendar and symbol universe contracts exist.

- [ ] **Step 1: Add failing tests**

Append to `tests/features/test_factor_quality.py`:

```python
def test_quality_analyzer_counts_symbols_and_post_warmup_null_ratio():
    values = [
        value(index=0, value_float=None, warmup_complete=False),
        value(index=1, value_float=None, warmup_complete=True),
        value(index=2, value_float=0.02, warmup_complete=True),
    ]

    report = FactorQualityAnalyzer().analyze(values, (spec("ret_1"),))

    assert metric(report, "ret_1", "ret_1", "symbol_count").metric_value == 1
    post_warmup = metric(report, "ret_1", "ret_1", "post_warmup_null_ratio")
    assert post_warmup.metric_value == 0.5
    assert post_warmup.metric_json == {"post_warmup_null_count": 1, "post_warmup_row_count": 2}


def test_quality_analyzer_computes_as_of_span_days():
    values = [
        value(index=0, value_float=0.01),
        value(index=2, value_float=0.02),
    ]

    report = FactorQualityAnalyzer().analyze(values, (spec("ret_1"),))

    span = metric(report, "ret_1", "ret_1", "as_of_span_days")
    assert span.metric_value == 2
    assert span.metric_json["as_of_min"] == "2026-07-01T07:00:00+00:00"
    assert span.metric_json["as_of_max"] == "2026-07-03T07:00:00+00:00"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/python -m pytest -v tests/features/test_factor_quality.py
```

Expected:

```text
StopIteration
```

because the new metrics are not emitted.

- [ ] **Step 3: Implement metric extensions**

Modify `src/quant_research/features/quality.py`.

Add import:

```python
from datetime import UTC, datetime
```

The file already imports this line. Keep one import line only.

Inside `_metrics_for_output`, after `row_count` calculation, add:

```python
        symbol_count = len({value.symbol for value in values})
        post_warmup_values = [value for value in values if value.warmup_complete]
        post_warmup_row_count = len(post_warmup_values)
        post_warmup_null_count = sum(
            1 for value in post_warmup_values if value.value_kind == "null"
        )
        post_warmup_null_ratio = (
            post_warmup_null_count / post_warmup_row_count
            if post_warmup_row_count
            else 0.0
        )
        as_of_values = [datetime.fromisoformat(value.as_of) for value in values]
        as_of_min = min(as_of_values).isoformat() if as_of_values else None
        as_of_max = max(as_of_values).isoformat() if as_of_values else None
        as_of_span_days = (
            float((max(as_of_values) - min(as_of_values)).days)
            if len(as_of_values) >= 2
            else 0.0
        )
```

In the returned metric list, after `row_count`, add:

```python
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "symbol_count",
                symbol_count,
                {},
                created_at,
            ),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "post_warmup_null_ratio",
                post_warmup_null_ratio,
                {
                    "post_warmup_null_count": post_warmup_null_count,
                    "post_warmup_row_count": post_warmup_row_count,
                },
                created_at,
            ),
            self._metric(
                spec.factor_id,
                output_field,
                values,
                "as_of_span_days",
                as_of_span_days,
                {
                    "as_of_min": as_of_min,
                    "as_of_max": as_of_max,
                },
                created_at,
            ),
```

- [ ] **Step 4: Update docs**

Modify `docs/development/factor-quality-checks.md`.

Add the new metrics to the list in Section 5:

```text
symbol_count
post_warmup_null_ratio
as_of_span_days
```

Add a short section:

```markdown
### 5.6 `symbol_count`

`symbol_count` is the number of unique symbols observed for one factor output. It is INFO-only in the current implementation.

### 5.7 `post_warmup_null_ratio`

`post_warmup_null_ratio` measures nulls after `warmup_complete=True`, separating natural rolling warmup nulls from later missing values. It is INFO-only until a dedicated threshold is added.

### 5.8 `as_of_span_days`

`as_of_span_days` records the calendar-day span between the minimum and maximum `as_of` values. The exact min and max timestamps are stored in `metric_json`.
```

- [ ] **Step 5: Run tests**

Run:

```bash
.venv/bin/python -m pytest -v tests/features/test_factor_quality.py
```

Expected:

```text
7 passed
```

- [ ] **Step 6: Commit**

```bash
git add src/quant_research/features/quality.py tests/features/test_factor_quality.py docs/development/factor-quality-checks.md
git commit -m "feat: add factor quality metric extensions"
```

---

### Task 7: Label Store MVP

**Files:**
- Create: `src/quant_research/labels/__init__.py`
- Create: `src/quant_research/labels/contracts.py`
- Create: `src/quant_research/labels/duckdb_store.py`
- Create: `tests/labels/test_duckdb_label_store.py`

Purpose: forward calculation outputs such as `forward_ret_1` should be first-class training labels, not normal live features.

- [ ] **Step 1: Write failing label store test**

Create `tests/labels/test_duckdb_label_store.py`:

```python
from quant_research.labels.contracts import LabelCommitRequest, LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore


def label_value(index: int, value_float: float | None) -> LabelValue:
    return LabelValue(
        label_run_id="label-run-1",
        label_set_id="next_return_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        label_id="forward_ret_1",
        label_version="1.0.0",
        value_float=value_float,
        value_kind="null" if value_float is None else "float",
        forward_bars=1,
        source_factor_run_id="factor-run-forward",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_label_store_commits_and_reads_labels(tmp_path):
    store = LocalDuckDBLabelStore(tmp_path / "research.duckdb")

    ref = store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="factor-run-forward",
            labels=(label_value(0, 0.01), label_value(1, None)),
        )
    )

    rows = store.read_labels(ref)
    manifest = store.get_manifest("label-run-1")

    assert ref.table == "label_table"
    assert len(rows) == 2
    assert rows[0].value_float == 0.01
    assert rows[1].value_kind == "null"
    assert manifest.row_count_label == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/labels/test_duckdb_label_store.py
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.labels'
```

- [ ] **Step 3: Implement contracts**

Create `src/quant_research/labels/__init__.py`:

```python
"""Training label contracts and storage adapters."""
```

Create `src/quant_research/labels/contracts.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LabelValue:
    label_run_id: str
    label_set_id: str
    dataset_id: str
    symbol: str
    freq: str
    as_of: str
    label_id: str
    label_version: str
    value_float: float | None
    value_kind: str
    forward_bars: int
    source_factor_run_id: str
    created_at: str


@dataclass(frozen=True)
class LabelCommitRequest:
    label_run_id: str
    label_set_id: str
    source_factor_run_id: str
    labels: tuple[LabelValue, ...]


@dataclass(frozen=True)
class LabelRunManifest:
    label_run_id: str
    label_set_id: str
    source_factor_run_id: str
    row_count_label: int
    status: str
    created_at: str
```

- [ ] **Step 4: Implement DuckDB label store**

Create `src/quant_research/labels/duckdb_store.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from quant_research.contracts.refs import DataRef
from quant_research.labels.contracts import LabelCommitRequest, LabelRunManifest, LabelValue


_LABEL_COLUMNS = (
    "label_run_id",
    "label_set_id",
    "dataset_id",
    "symbol",
    "freq",
    "as_of",
    "label_id",
    "label_version",
    "value_float",
    "value_kind",
    "forward_bars",
    "source_factor_run_id",
    "created_at",
)


class LocalDuckDBLabelStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def commit_labels(self, request: LabelCommitRequest) -> DataRef:
        manifest = LabelRunManifest(
            label_run_id=request.label_run_id,
            label_set_id=request.label_set_id,
            source_factor_run_id=request.source_factor_run_id,
            row_count_label=len(request.labels),
            status="COMMITTED",
            created_at=datetime.now(UTC).isoformat(),
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM label_table WHERE label_run_id = ?", [request.label_run_id])
                conn.execute("DELETE FROM label_run_manifest WHERE label_run_id = ?", [request.label_run_id])
                placeholders = ", ".join(["?"] * len(_LABEL_COLUMNS))
                conn.executemany(
                    f"""
                    INSERT INTO label_table ({", ".join(_LABEL_COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    [self._label_to_row(label) for label in request.labels],
                )
                conn.execute(
                    """
                    INSERT INTO label_run_manifest (
                        label_run_id, label_set_id, source_factor_run_id,
                        row_count_label, status, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        manifest.label_run_id,
                        manifest.label_set_id,
                        manifest.source_factor_run_id,
                        manifest.row_count_label,
                        manifest.status,
                        manifest.created_at,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return DataRef("label_table", {"label_run_id": request.label_run_id})

    def read_labels(self, ref: DataRef | str) -> list[LabelValue]:
        data_ref = DataRef.parse(ref) if isinstance(ref, str) else ref
        if data_ref.table != "label_table":
            raise ValueError(f"unsupported label table ref: {data_ref.table}")
        label_run_id = data_ref.filters["label_run_id"]
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {", ".join(_LABEL_COLUMNS)}
                FROM label_table
                WHERE label_run_id = ?
                ORDER BY symbol, as_of, label_id
                """,
                [label_run_id],
            ).fetchall()
        return [self._row_to_label(row) for row in rows]

    def get_manifest(self, label_run_id: str) -> LabelRunManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT label_run_id, label_set_id, source_factor_run_id,
                       row_count_label, status, created_at
                FROM label_run_manifest
                WHERE label_run_id = ?
                """,
                [label_run_id],
            ).fetchone()
        if row is None:
            return None
        return LabelRunManifest(
            label_run_id=row[0],
            label_set_id=row[1],
            source_factor_run_id=row[2],
            row_count_label=row[3],
            status=row[4],
            created_at=row[5],
        )

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS label_table (
                    label_run_id VARCHAR NOT NULL,
                    label_set_id VARCHAR NOT NULL,
                    dataset_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    freq VARCHAR NOT NULL,
                    as_of VARCHAR NOT NULL,
                    label_id VARCHAR NOT NULL,
                    label_version VARCHAR NOT NULL,
                    value_float DOUBLE,
                    value_kind VARCHAR NOT NULL,
                    forward_bars BIGINT NOT NULL,
                    source_factor_run_id VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS label_run_manifest (
                    label_run_id VARCHAR PRIMARY KEY,
                    label_set_id VARCHAR NOT NULL,
                    source_factor_run_id VARCHAR NOT NULL,
                    row_count_label BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL
                )
                """
            )

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _label_to_row(self, label: LabelValue) -> tuple[object, ...]:
        return (
            label.label_run_id,
            label.label_set_id,
            label.dataset_id,
            label.symbol,
            label.freq,
            label.as_of,
            label.label_id,
            label.label_version,
            label.value_float,
            label.value_kind,
            label.forward_bars,
            label.source_factor_run_id,
            label.created_at,
        )

    def _row_to_label(self, row) -> LabelValue:
        return LabelValue(
            label_run_id=row[0],
            label_set_id=row[1],
            dataset_id=row[2],
            symbol=row[3],
            freq=row[4],
            as_of=row[5],
            label_id=row[6],
            label_version=row[7],
            value_float=row[8],
            value_kind=row[9],
            forward_bars=row[10],
            source_factor_run_id=row[11],
            created_at=row[12],
        )
```

- [ ] **Step 5: Run label tests**

Run:

```bash
.venv/bin/python -m pytest -v tests/labels/test_duckdb_label_store.py
```

Expected:

```text
1 passed
```

- [ ] **Step 6: Commit**

```bash
git add src/quant_research/labels tests/labels/test_duckdb_label_store.py
git commit -m "feat: add duckdb label store"
```

---

### Task 8: Feature Matrix Builder

**Files:**
- Create: `src/quant_research/datasets/__init__.py`
- Create: `src/quant_research/datasets/feature_matrix.py`
- Create: `tests/datasets/test_feature_matrix.py`

Purpose: downstream research should consume a table-shaped matrix after quality gates, not raw JSON snapshots.

- [ ] **Step 1: Write failing matrix test**

Create `tests/datasets/test_feature_matrix.py`:

```python
from quant_research.datasets.feature_matrix import snapshots_to_feature_matrix
from quant_research.features.contracts import FeatureSnapshot


def snapshot(index: int, features: dict[str, object]) -> FeatureSnapshot:
    return FeatureSnapshot(
        snapshot_id=f"snapshot-{index}",
        feature_set_id="basic_price_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        features=features,
        factor_run_ids=("factor-run-1",),
        input_data_refs=("duckdb://curated_market_bar?dataset_id=fixture-daily",),
        warmup_complete=index > 0,
        quality_flags=(),
        feature_ref=f"duckdb://feature_snapshot?snapshot_id=snapshot-{index}",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_snapshots_to_feature_matrix_expands_feature_json():
    frame = snapshots_to_feature_matrix(
        [
            snapshot(0, {"ret_1": None, "ma_3": None}),
            snapshot(1, {"ret_1": 0.01, "ma_3": 10.2}),
        ]
    ).collect()

    assert frame.columns == [
        "dataset_id",
        "symbol",
        "freq",
        "as_of",
        "warmup_complete",
        "ret_1",
        "ma_3",
    ]
    assert frame["ret_1"].to_list() == [None, 0.01]
    assert frame["ma_3"].to_list() == [None, 10.2]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest -v tests/datasets/test_feature_matrix.py
```

Expected:

```text
ModuleNotFoundError: No module named 'quant_research.datasets'
```

- [ ] **Step 3: Implement matrix builder**

Create `src/quant_research/datasets/__init__.py`:

```python
"""Training dataset builders."""
```

Create `src/quant_research/datasets/feature_matrix.py`:

```python
from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from quant_research.features.contracts import FeatureSnapshot


def snapshots_to_feature_matrix(snapshots: Iterable[FeatureSnapshot]) -> pl.LazyFrame:
    rows = []
    for snapshot in snapshots:
        row = {
            "dataset_id": snapshot.dataset_id,
            "symbol": snapshot.symbol,
            "freq": snapshot.freq,
            "as_of": snapshot.as_of,
            "warmup_complete": snapshot.warmup_complete,
        }
        row.update(snapshot.features)
        rows.append(row)
    return pl.DataFrame(rows).lazy()
```

- [ ] **Step 4: Run test**

Run:

```bash
.venv/bin/python -m pytest -v tests/datasets/test_feature_matrix.py
```

Expected:

```text
1 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/quant_research/datasets tests/datasets/test_feature_matrix.py
git commit -m "feat: add feature matrix builder"
```

---

### Task 9: Development Documentation

**Files:**
- Create: `docs/development/research-pipeline-development.md`
- Modify: `README.md`
- Modify: `docs/development/factor-quality-checks.md`
- Modify: `docs/development/feature-store-spec.md`

- [ ] **Step 1: Create pipeline development doc**

Create `docs/development/research-pipeline-development.md`:

````markdown
# Research Pipeline Development Guide

## Purpose

The research pipeline connects existing decoupled modules without merging their responsibilities:

```text
curated bars -> Polars factors -> FeatureStore -> factor quality report -> gated feature consumption
```

## Current Entry Point

```python
ResearchPipelineService(
    data_store=data_store,
    feature_store=feature_store,
    factor_registry=registry,
).run(ResearchPipelineRequest(config=config))
```

## Quality Gate Rule

`FeatureRunStatus.COMMITTED` means rows were written. `quality_status` decides whether those rows are usable:

| quality_status | Default consumption |
|---|---|
| `PASSED` | Allowed |
| `WARNING` | Allowed only when explicitly configured |
| `FAILED` | Blocked |
| `NOT_RUN` | Blocked |

## Forward Calculations

Forward calculations are labels. If a factor declares `forward_bars > 0`, `uses_future_data = true`, or `causal = false`, the feature quality layer marks it with `future_leakage_count > 0`. Such runs are not usable as live feature snapshots.

## Next Extensions

1. Add label store routing for forward calculations.
2. Build model-ready feature matrices from quality-gated snapshots.
3. Add row-level input lineage to strengthen leakage detection.
````

- [ ] **Step 2: Update README docs list**

Modify `README.md` Development docs list to include:

```markdown
- `docs/development/research-pipeline-development.md`
```

Modify Implemented entry points after Task 3:

```markdown
- `quant_research.pipeline.research.ResearchPipelineService`
```

Modify Implemented entry points after Task 8:

```markdown
- `quant_research.datasets.feature_matrix.snapshots_to_feature_matrix`
```

- [ ] **Step 3: Update quality docs**

Modify `docs/development/factor-quality-checks.md` by adding under Call Pattern:

```markdown
The research pipeline should call `commit_quality_report(...)` immediately after a successful `commit_run(...)`. Downstream readers must not infer usability from `FeatureRunStatus.COMMITTED`; they must check `factor_run_manifest.quality_status`.
```

- [ ] **Step 4: Update FeatureStore docs**

Modify `docs/development/feature-store-spec.md` by adding under Responsibilities:

```markdown
FeatureStore records quality status, but consumer-side code owns the decision to block reads. Use `require_usable_manifest(...)` before building research matrices or quasi-live inputs.
```

- [ ] **Step 5: Verify documentation references**

Run:

```bash
rg -n "ResearchPipelineService|research-pipeline-development|require_usable_manifest|quality_status" README.md docs/development
```

Expected:

```text
README.md
docs/development/research-pipeline-development.md
docs/development/factor-quality-checks.md
docs/development/feature-store-spec.md
```

- [ ] **Step 6: Commit**

```bash
git add README.md docs/development/research-pipeline-development.md docs/development/factor-quality-checks.md docs/development/feature-store-spec.md
git commit -m "docs: add research pipeline development guide"
```

---

### Task 10: Full Verification and Branch Finish

**Files:**
- No code files unless verification reveals a failure.

- [ ] **Step 1: Run full test suite**

Run:

```bash
.venv/bin/python -m pytest -v
```

Expected:

```text
all tests passed
```

- [ ] **Step 2: Run ruff**

Run:

```bash
.venv/bin/ruff check src tests
```

Expected:

```text
All checks passed!
```

- [ ] **Step 3: Check whitespace**

Run:

```bash
git diff --check
```

Expected:

```text
no output
```

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
```

Expected:

```text
current branch contains the task commits
no unstaged changes remain
```

- [ ] **Step 5: Push**

Run:

```bash
git push
```

Expected:

```text
feature branch pushed to origin
```

## Acceptance Checklist

M1 is complete when:

- `ResearchPipelineService.run(...)` reads bars by `DataRef`.
- It computes factors through `PolarsFactorRunner`.
- It commits features through `LocalDuckDBFeatureStore`.
- It writes a quality report through `commit_quality_report(...)`.
- It returns `COMMITTED` for clean feature runs.
- It returns `QUALITY_FAILED` for strict-quality forward/label outputs.

M2 is complete when:

- Consumer code can call `require_usable_manifest(...)`.
- `FAILED` and `NOT_RUN` runs are blocked by default.
- `WARNING` is blocked unless `allow_warning=True`.

M3 is complete when:

- The quality analyzer emits `symbol_count`, `post_warmup_null_ratio`, and `as_of_span_days`.
- Existing metrics still pass all tests.

M4 is complete when:

- Forward label rows can be stored in `label_table`.
- Label manifests preserve `source_factor_run_id` lineage.

M5 is complete when:

- Feature snapshots can be expanded into a Polars feature matrix.
- The matrix keeps keys and warmup state.

## Risk Notes

- Current quality checks are post-commit. This is acceptable for research traceability, but downstream readers must gate on `quality_status`.
- `future_leakage_count` is metadata-level today. Row-level leakage requires `input_window_start` / `input_window_end` lineage.
- Existing DuckDB files may need schema migration before reuse after adding new tables or fields.
- LabelStore should not be mixed into FeatureStore; labels and live features have different consumption semantics.

## Suggested Commit Sequence

```text
feat: add bar frame adapter
feat: add research pipeline contracts
feat: add research pipeline service
test: cover strict factor quality gate
feat: add feature quality consumption gate
feat: add factor quality metric extensions
feat: add duckdb label store
feat: add feature matrix builder
docs: add research pipeline development guide
```
