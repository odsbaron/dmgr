from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import typer
import yaml

from quant_research.contracts.bar import Adjustment, Frequency
from quant_research.contracts.refs import DataRef
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.duckdb_store import LocalDuckDBStore
from quant_research.data.ingestion import DataIngestionService, IngestionResult
from quant_research.data.quality import KLineQualityValidator
from quant_research.factors.builtin import default_factor_registry
from quant_research.factors.polars import PolarsFactorRunner
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.quality import FactorQualityAnalyzer
from quant_research.pipeline.contracts import (
    ResearchRunRequest,
    ResearchRunResult,
    ResearchRunStatus,
)
from quant_research.pipeline.research import ResearchPipeline


app = typer.Typer(
    name="quant-research",
    help="Local DuckDB and Polars batch research workflow.",
    no_args_is_help=True,
)


@app.command("ingest-bars")
def ingest_bars(
    input_path: Path = typer.Option(..., "--input", exists=True, dir_okay=False),
    freq: str = typer.Option(..., "--freq"),
    dataset: str = typer.Option(..., "--dataset"),
    db: Path = typer.Option(Path("data/research.duckdb"), "--db"),
    source: str | None = typer.Option(None, "--source"),
    timezone: str = typer.Option("Asia/Shanghai", "--timezone"),
    calendar: str = typer.Option("cn_stock_simple", "--calendar"),
    adjustment: str = typer.Option("NONE", "--adjustment"),
    export: Path | None = typer.Option(None, "--export"),
) -> None:
    """Normalize and validate a CSV/Parquet K-line file, then write curated bars."""
    store = LocalDuckDBStore(db)
    result = _ingest(
        store,
        input_path=input_path,
        freq=_frequency(freq),
        dataset=dataset,
        source=source,
        timezone=timezone,
        calendar=calendar,
        adjustment=_adjustment(adjustment),
    )
    payload = _ingestion_payload(result)
    if export is not None and result.data_ref is not None:
        payload["export_path"] = str(store.export_bars_to_parquet(result.data_ref, export))
    _echo(payload)
    if result.data_ref is None:
        raise typer.Exit(code=1)


@app.command("validate-bars")
def validate_bars(
    dataset: str = typer.Option(..., "--dataset"),
    freq: str = typer.Option(..., "--freq"),
    db: Path = typer.Option(Path("data/research.duckdb"), "--db"),
    source_run_id: str | None = typer.Option(None, "--source-run-id"),
    timezone: str = typer.Option("Asia/Shanghai", "--timezone"),
    calendar: str = typer.Option("cn_stock_simple", "--calendar"),
) -> None:
    """Re-run machine-readable K-line quality checks for a curated table slice."""
    filters = {"dataset_id": dataset, "freq": _frequency(freq).value}
    if source_run_id:
        filters["source_run_id"] = source_run_id
    ref = DataRef("curated_market_bar", filters)
    bars = LocalDuckDBStore(db).read_bars(ref)
    report = KLineQualityValidator(
        import_run_id=source_run_id or "validation",
        calendar_id=calendar,
        timezone=timezone,
    ).validate(bars)
    _echo(
        {
            "data_ref": ref.uri,
            "row_count": len(bars),
            "status": "FAILED" if report.has_blocking_errors else "PASSED",
            "issue_count": report.issue_count,
            "issues": [_quality_issue_payload(issue) for issue in report.issues],
        }
    )
    if report.has_blocking_errors:
        raise typer.Exit(code=1)


@app.command("compute-factors")
def compute_factors(
    dataset: str = typer.Option(..., "--dataset"),
    feature_set: str = typer.Option(..., "--feature-set"),
    freq: str = typer.Option(..., "--freq"),
    factors: str = typer.Option("ret_1", "--factors"),
    db: Path = typer.Option(Path("data/research.duckdb"), "--db"),
    data_ref: str | None = typer.Option(None, "--data-ref"),
    factor_run_id: str | None = typer.Option(None, "--factor-run-id"),
) -> None:
    """Compute registered built-in factors and persist feature quality artifacts."""
    parsed_freq = _frequency(freq)
    result = _run_research(
        db=db,
        data_ref=data_ref
        or DataRef(
            "curated_market_bar",
            {"dataset_id": dataset, "freq": parsed_freq.value},
        ).uri,
        feature_set=feature_set,
        factors=_factor_ids(factors),
        factor_run_id=factor_run_id or str(uuid4()),
    )
    _echo(_research_payload(result))
    if result.status != ResearchRunStatus.COMMITTED:
        raise typer.Exit(code=1)


@app.command("run-pipeline")
def run_pipeline(
    config: Path = typer.Option(..., "--config", exists=True, dir_okay=False),
) -> None:
    """Execute ingest, validation, factor computation, quality, and manifest writes."""
    payload = _load_config(config)
    base = config.parent
    input_path = _resolved_path(base, _required(payload, "input"))
    db = _resolved_path(base, payload.get("database", "data/research.duckdb"))
    dataset = str(_required(payload, "dataset"))
    parsed_freq = _frequency(str(_required(payload, "freq")))
    store = LocalDuckDBStore(db)
    ingestion = _ingest(
        store,
        input_path=input_path,
        freq=parsed_freq,
        dataset=dataset,
        source=_optional_text(payload.get("source")),
        timezone=str(payload.get("timezone", "Asia/Shanghai")),
        calendar=str(payload.get("calendar", "cn_stock_simple")),
        adjustment=_adjustment(str(payload.get("adjustment", "NONE"))),
    )
    if ingestion.data_ref is None:
        _echo({"ingestion": _ingestion_payload(ingestion), "research": None})
        raise typer.Exit(code=1)

    export_path = None
    if payload.get("export"):
        export_path = store.export_bars_to_parquet(
            ingestion.data_ref,
            _resolved_path(base, payload["export"]),
        )
    factor_ids = _factor_ids(payload.get("factors", ["ret_1"]))
    research = _run_research(
        db=db,
        data_ref=ingestion.data_ref.uri,
        feature_set=str(payload.get("feature_set", "basic-v1")),
        factors=factor_ids,
        factor_run_id=str(payload.get("factor_run_id") or uuid4()),
    )
    result_payload: dict[str, Any] = {
        "ingestion": _ingestion_payload(ingestion),
        "research": _research_payload(research),
    }
    if export_path is not None:
        result_payload["export_path"] = str(export_path)
    _echo(result_payload)
    if research.status != ResearchRunStatus.COMMITTED:
        raise typer.Exit(code=1)


def _ingest(
    store: LocalDuckDBStore,
    *,
    input_path: Path,
    freq: Frequency,
    dataset: str,
    source: str | None,
    timezone: str,
    calendar: str,
    adjustment: Adjustment,
) -> IngestionResult:
    spec = SourceSpec(
        source_id=source or f"local-{input_path.stem}",
        dataset_id=dataset,
        source_type=(
            SourceType.PARQUET if input_path.suffix.lower() == ".parquet" else SourceType.CSV
        ),
        path=str(input_path),
        freq=freq,
        timezone=timezone,
        adjustment=adjustment,
        field_mapping={
            "symbol": "symbol",
            "exchange": "exchange",
            "date": "date",
            "datetime": "datetime",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "turnover": "turnover",
        },
        calendar_id=calendar,
    )
    return DataIngestionService(store).ingest(spec)


def _run_research(
    *,
    db: Path,
    data_ref: str,
    feature_set: str,
    factors: tuple[str, ...],
    factor_run_id: str,
) -> ResearchRunResult:
    registry = default_factor_registry()
    pipeline = ResearchPipeline(
        data_store=LocalDuckDBStore(db),
        factor_registry=registry,
        factor_runner=PolarsFactorRunner(registry),
        feature_store=LocalDuckDBFeatureStore(db),
        quality_analyzer=FactorQualityAnalyzer(),
    )
    return pipeline.run(
        ResearchRunRequest(
            factor_run_id=factor_run_id,
            feature_set_id=feature_set,
            input_data_ref=data_ref,
            factor_ids=factors,
        )
    )


def _ingestion_payload(result: IngestionResult) -> dict[str, Any]:
    return {
        "import_run_id": result.import_run_id,
        "status": result.status.value,
        "data_ref": result.data_ref.uri if result.data_ref else None,
        "row_count_raw": result.row_count_raw,
        "row_count_curated": result.row_count_curated,
        "issue_count": result.quality_report.issue_count,
        "reused_existing": result.reused_existing,
    }


def _research_payload(result: ResearchRunResult) -> dict[str, Any]:
    return {
        "factor_run_id": result.factor_run_id,
        "status": result.status.value,
        "feature_table_ref": result.feature_table_ref.uri if result.feature_table_ref else None,
        "snapshot_ref": result.snapshot_ref.uri if result.snapshot_ref else None,
        "manifest_ref": result.manifest_ref.uri if result.manifest_ref else None,
        "quality_status": result.quality_status.value,
        "quality_summary": result.quality_summary,
        "consumable": result.consumable,
        "row_count_input": result.row_count_input,
        "row_count_feature": result.row_count_feature,
        "row_count_snapshot": result.row_count_snapshot,
        "metric_count": result.metric_count,
        "error_step": result.error_step,
        "error_code": result.error_code,
        "error_message": result.error_message,
    }


def _quality_issue_payload(issue) -> dict[str, Any]:
    return {
        "issue_code": issue.issue_code,
        "symbol": issue.symbol,
        "trading_date": issue.trading_date.isoformat() if issue.trading_date else None,
        "bar_start_time": issue.bar_start_time.isoformat() if issue.bar_start_time else None,
        "message": issue.message,
    }


def _frequency(value: str) -> Frequency:
    try:
        return Frequency(value)
    except ValueError as exc:
        raise typer.BadParameter(f"unsupported frequency: {value}") from exc


def _adjustment(value: str) -> Adjustment:
    try:
        return Adjustment(value.upper())
    except ValueError as exc:
        raise typer.BadParameter(f"unsupported adjustment: {value}") from exc


def _factor_ids(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        result = tuple(item.strip() for item in value.split(",") if item.strip())
    elif isinstance(value, list | tuple):
        result = tuple(str(item).strip() for item in value if str(item).strip())
    else:
        result = ()
    if not result:
        raise typer.BadParameter("at least one factor id is required")
    return result


def _load_config(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise typer.BadParameter("pipeline config must be a YAML mapping")
    return loaded


def _required(payload: dict[str, Any], field: str) -> object:
    value = payload.get(field)
    if value is None or str(value).strip() == "":
        raise typer.BadParameter(f"pipeline config requires {field}")
    return value


def _resolved_path(base: Path, value: object) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else base / path


def _optional_text(value: object) -> str | None:
    return str(value) if value is not None and str(value).strip() else None


def _echo(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, sort_keys=True, ensure_ascii=False))
