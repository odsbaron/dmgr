import csv
import json
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.contracts.bar import AssetClass
from quant_research.daily_status import (
    BarExpectation,
    DailyStatusDefinition,
    DailyStatusIngestionService,
    DailyStatusResolver,
    DailyStatusSourceSpec,
    DailyStatusStoreError,
    LocalDuckDBDailyStatusStore,
    MarketState,
    StatusImportStatus,
    StatusSourceType,
)


def definition() -> DailyStatusDefinition:
    return DailyStatusDefinition(
        status_id="ashare-daily-status",
        version="v1",
        name="A-share daily market state",
        asset_class=AssetClass.EQUITY,
        calendar_id="xshg-xshe",
        calendar_version="v1",
        timezone="Asia/Shanghai",
    )


def rows(*, suspended_expectation="NO_BARS") -> list[dict[str, object]]:
    return [
        {
            "trading_date": "2026-07-07",
            "instrument_id": "000001.SZ",
            "market_state": "ACTIVE",
            "bar_expectation": "FULL_SESSION",
            "custom_intervals": "",
        },
        {
            "trading_date": "2026-07-07",
            "instrument_id": "000002.SZ",
            "market_state": "SUSPENDED",
            "bar_expectation": suspended_expectation,
            "custom_intervals": "",
        },
    ]


def write_csv(path, values) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(values[0]))
        writer.writeheader()
        writer.writerows(values)


def spec(path, source_type, *, source_id="status-source") -> DailyStatusSourceSpec:
    return DailyStatusSourceSpec(
        source_id=source_id,
        status_id="ashare-daily-status",
        status_version="v1",
        source_type=source_type,
        path=str(path),
        trading_date=date(2026, 7, 7),
        known_at=datetime(2026, 7, 7, 1, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 7, 0, 0, tzinfo=UTC),
        field_mapping={
            "trading_date": "trading_date",
            "instrument_id": "instrument_id",
            "market_state": "market_state",
            "bar_expectation": "bar_expectation",
            "custom_intervals": "custom_intervals",
        },
    )


def test_status_ingestion_preserves_suspension_without_market_bars(tmp_path):
    path = tmp_path / "status.csv"
    write_csv(path, rows())
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")
    result = DailyStatusIngestionService(
        store,
        run_id_factory=lambda: "status-run-1",
    ).ingest(definition(), spec(path, StatusSourceType.CSV))
    snapshot_set = store.create_snapshot_set(
        status_id="ashare-daily-status",
        status_version="v1",
        trading_dates=(date(2026, 7, 7),),
    )
    resolved = DailyStatusResolver(store).resolve(snapshot_set.ref)
    suspended = resolved.statuses_by_date[date(2026, 7, 7)]["000002.SZ"]

    assert result.status == StatusImportStatus.COMMITTED
    assert suspended.market_state == MarketState.SUSPENDED
    assert suspended.bar_expectation == BarExpectation.NO_BARS
    assert resolved.asset_class == AssetClass.EQUITY
    assert resolved.calendar_id == "xshg-xshe"


def test_equivalent_csv_and_parquet_reuse_canonical_status_snapshot(tmp_path):
    csv_path = tmp_path / "status.csv"
    parquet_path = tmp_path / "status.parquet"
    write_csv(csv_path, rows())
    pl.DataFrame(rows()).write_parquet(parquet_path)
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")
    run_ids = iter(["csv-run", "parquet-run"])
    service = DailyStatusIngestionService(store, run_id_factory=lambda: next(run_ids))

    first = service.ingest(definition(), spec(csv_path, StatusSourceType.CSV, source_id="csv"))
    second = service.ingest(
        definition(),
        spec(parquet_path, StatusSourceType.PARQUET, source_id="parquet"),
    )

    assert first.snapshot_id == second.snapshot_id
    assert first.content_hash == second.content_hash
    assert second.reused_existing is True


def test_custom_intervals_are_parsed_and_resolved(tmp_path):
    path = tmp_path / "custom.csv"
    values = [
        {
            "trading_date": "2026-07-07",
            "instrument_id": "000001.SZ",
            "market_state": "ACTIVE",
            "bar_expectation": "CUSTOM_INTERVALS",
            "custom_intervals": json.dumps(
                [
                    {"start_time": "10:00:00", "end_time": "11:00:00"},
                    {"start_time": "14:00:00", "end_time": "14:30:00"},
                ]
            ),
        }
    ]
    write_csv(path, values)
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")
    result = DailyStatusIngestionService(store, run_id_factory=lambda: "custom-run").ingest(
        definition(), spec(path, StatusSourceType.CSV)
    )
    status = store.get_snapshot(result.snapshot_id).statuses[0]

    assert result.status == StatusImportStatus.COMMITTED
    assert status.bar_expectation == BarExpectation.CUSTOM_INTERVALS
    assert [(item.start_time.isoformat(), item.end_time.isoformat()) for item in status.custom_intervals] == [
        ("10:00:00", "11:00:00"),
        ("14:00:00", "14:30:00"),
    ]


def test_duplicate_instrument_status_fails_and_run_is_retained(tmp_path):
    path = tmp_path / "duplicate.csv"
    duplicate = rows()
    duplicate.append(dict(duplicate[0]))
    write_csv(path, duplicate)
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")
    result = DailyStatusIngestionService(store, run_id_factory=lambda: "duplicate-run").ingest(
        definition(), spec(path, StatusSourceType.CSV)
    )

    assert result.status == StatusImportStatus.FAILED
    assert result.error_code == "QUALITY_GATE_FAILED"
    assert store.get_import_run("duplicate-run").status == StatusImportStatus.FAILED
    assert "DUPLICATE_STATUS" in {
        item.issue_code for item in store.list_quality_issues("duplicate-run")
    }


def test_no_bars_rejects_custom_intervals(tmp_path):
    path = tmp_path / "invalid-intervals.csv"
    values = [
        {
            "trading_date": "2026-07-07",
            "instrument_id": "000002.SZ",
            "market_state": "SUSPENDED",
            "bar_expectation": "NO_BARS",
            "custom_intervals": "10:00:00-11:00:00",
        }
    ]
    write_csv(path, values)
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")

    result = DailyStatusIngestionService(store, run_id_factory=lambda: "invalid-run").ingest(
        definition(), spec(path, StatusSourceType.CSV)
    )

    assert result.status == StatusImportStatus.FAILED
    assert {item.issue_code for item in result.quality_report.issues} == {
        "UNEXPECTED_CUSTOM_INTERVALS"
    }


def test_status_definition_and_historical_partition_conflicts_are_rejected(tmp_path):
    original = tmp_path / "original.csv"
    changed = tmp_path / "changed.csv"
    write_csv(original, rows())
    write_csv(changed, rows(suspended_expectation="UNKNOWN"))
    store = LocalDuckDBDailyStatusStore(tmp_path / "research.duckdb")
    run_ids = iter(["first", "changed"])
    service = DailyStatusIngestionService(store, run_id_factory=lambda: next(run_ids))
    first = service.ingest(definition(), spec(original, StatusSourceType.CSV, source_id="first"))
    conflict = service.ingest(
        definition(), spec(changed, StatusSourceType.CSV, source_id="changed")
    )

    assert first.status == StatusImportStatus.COMMITTED
    assert conflict.error_code == "IMMUTABLE_PARTITION_CONFLICT"
    assert store.get_snapshot(first.snapshot_id).content_hash == first.content_hash
    with pytest.raises(DailyStatusStoreError) as exc_info:
        store.register_definition(replace(definition(), calendar_version="v2"))
    assert exc_info.value.code == "DEFINITION_CONFLICT"
