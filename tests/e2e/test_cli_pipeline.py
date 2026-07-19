import json
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from quant_research.cli import app
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore


runner = CliRunner()


def output_json(result) -> dict[str, object]:
    assert result.stdout.strip(), result.exception
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_cli_ingest_validate_and_compute_factor_commands(tmp_path):
    db = tmp_path / "research.duckdb"
    fixture = Path("tests/fixtures/bars_daily.csv").resolve()

    ingest = runner.invoke(
        app,
        [
            "ingest-bars",
            "--input",
            str(fixture),
            "--freq",
            "1d",
            "--dataset",
            "cli-daily",
            "--db",
            str(db),
        ],
    )
    ingest_payload = output_json(ingest)
    assert ingest.exit_code == 0, ingest.exception
    assert ingest_payload["status"] == "COMMITTED"

    validate = runner.invoke(
        app,
        [
            "validate-bars",
            "--dataset",
            "cli-daily",
            "--freq",
            "1d",
            "--db",
            str(db),
        ],
    )
    validate_payload = output_json(validate)
    assert validate.exit_code == 0, validate.exception
    assert validate_payload["status"] == "PASSED"
    assert validate_payload["row_count"] == 2

    compute = runner.invoke(
        app,
        [
            "compute-factors",
            "--dataset",
            "cli-daily",
            "--feature-set",
            "basic-v1",
            "--freq",
            "1d",
            "--factors",
            "ret_1",
            "--db",
            str(db),
            "--factor-run-id",
            "cli-factor-run",
        ],
    )
    compute_payload = output_json(compute)
    assert compute.exit_code == 0, compute.exception
    assert compute_payload["status"] == "COMMITTED"
    manifest = LocalDuckDBFeatureStore(db).get_manifest("cli-factor-run")
    assert manifest is not None
    assert manifest.quality_status == "PASSED"
    assert manifest.quality_report_ref is not None


@pytest.mark.parametrize(
    ("fixture_name", "freq", "dataset"),
    [
        ("bars_daily.csv", "1d", "e2e-daily"),
        ("bars_1m.csv", "1m", "e2e-minute"),
    ],
)
def test_config_driven_pipeline_ingests_computes_and_writes_manifest(
    tmp_path,
    fixture_name,
    freq,
    dataset,
):
    db = tmp_path / f"{freq}.duckdb"
    export = tmp_path / "exports" / f"{dataset}.parquet"
    config = tmp_path / f"pipeline-{freq}.yml"
    config.write_text(
        yaml.safe_dump(
            {
                "database": str(db),
                "input": str(Path("tests/fixtures", fixture_name).resolve()),
                "dataset": dataset,
                "freq": freq,
                "source": f"fixture-{freq}",
                "timezone": "Asia/Shanghai",
                "calendar": "cn_stock_simple",
                "feature_set": "basic-v1",
                "factors": ["ret_1"],
                "factor_run_id": f"e2e-{freq}-run",
                "export": str(export),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["run-pipeline", "--config", str(config)])
    payload = output_json(result)

    assert result.exit_code == 0, result.exception
    assert payload["ingestion"]["status"] == "COMMITTED"
    assert payload["research"]["status"] == "COMMITTED"
    assert payload["research"]["quality_status"] == "PASSED"
    assert payload["research"]["manifest_ref"] is not None
    assert export.exists()

    feature_store = LocalDuckDBFeatureStore(db)
    manifest = feature_store.get_manifest(f"e2e-{freq}-run")
    snapshots = feature_store.read_snapshot(payload["research"]["snapshot_ref"])
    assert manifest is not None
    assert manifest.status.value == "COMMITTED"
    assert manifest.config_hash.startswith("sha256:")
    assert len(snapshots) == 2
    if freq == "1m":
        first = datetime.fromisoformat(snapshots[0].as_of)
        second = datetime.fromisoformat(snapshots[1].as_of)
        assert (second - first).total_seconds() == 60
