from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.market_calendar import (
    CalendarImportStatus,
    CalendarIngestionService,
    CalendarResolver,
    CalendarSourceSpec,
    CalendarSourceType,
    CalendarStoreError,
    LocalDuckDBCalendarStore,
    MarketCalendarDefinition,
)


def definition() -> MarketCalendarDefinition:
    return MarketCalendarDefinition(
        calendar_id="xshg-xshe",
        version="v1",
        name="Shanghai and Shenzhen exchanges",
        timezone="Asia/Shanghai",
    )


def rows(*, afternoon_end: str = "15:00:00") -> list[dict[str, object]]:
    return [
        {
            "calendar_date": "2026-07-07",
            "is_trading_day": True,
            "session_id": "morning",
            "session_start": "09:30:00",
            "session_end": "11:30:00",
            "session_kind": "REGULAR",
        },
        {
            "calendar_date": "2026-07-07",
            "is_trading_day": True,
            "session_id": "afternoon",
            "session_start": "13:00:00",
            "session_end": afternoon_end,
            "session_kind": "REGULAR",
        },
    ]


def write_csv(path, values) -> None:
    columns = list(values[0])
    path.write_text(
        "\n".join(
            [
                ",".join(columns),
                *[
                    ",".join(str(row[column]).lower() if isinstance(row[column], bool) else str(row[column]) for column in columns)
                    for row in values
                ],
            ]
        ),
        encoding="utf-8",
    )


def spec(path, source_type, *, source_id="calendar-source") -> CalendarSourceSpec:
    return CalendarSourceSpec(
        source_id=source_id,
        calendar_id="xshg-xshe",
        calendar_version="v1",
        source_type=source_type,
        path=str(path),
        calendar_date=date(2026, 7, 7),
        known_at=datetime(2026, 7, 7, 0, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 6, 23, 0, tzinfo=UTC),
        field_mapping={
            "calendar_date": "calendar_date",
            "is_trading_day": "is_trading_day",
            "session_id": "session_id",
            "session_start": "session_start",
            "session_end": "session_end",
            "session_kind": "session_kind",
        },
    )


def test_split_session_csv_ingestion_and_exact_resolution(tmp_path):
    path = tmp_path / "calendar.csv"
    write_csv(path, rows())
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")
    result = CalendarIngestionService(
        store,
        run_id_factory=lambda: "calendar-run-1",
    ).ingest(definition(), spec(path, CalendarSourceType.CSV))
    snapshot_set = store.create_snapshot_set(
        calendar_id="xshg-xshe",
        calendar_version="v1",
        calendar_dates=(date(2026, 7, 7),),
    )
    resolved = CalendarResolver(store).resolve(snapshot_set.ref)
    day = resolved.days_by_date[date(2026, 7, 7)]

    assert result.status == CalendarImportStatus.COMMITTED
    assert day.is_trading_day is True
    assert [(item.start_time.isoformat(), item.end_time.isoformat()) for item in day.sessions] == [
        ("09:30:00", "11:30:00"),
        ("13:00:00", "15:00:00"),
    ]
    assert resolved.snapshot_set_hash == snapshot_set.snapshot_set_hash


def test_equivalent_csv_and_parquet_reuse_canonical_snapshot(tmp_path):
    csv_path = tmp_path / "calendar.csv"
    parquet_path = tmp_path / "calendar.parquet"
    write_csv(csv_path, rows())
    pl.DataFrame(rows()).write_parquet(parquet_path)
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")
    run_ids = iter(["csv-run", "parquet-run"])
    service = CalendarIngestionService(store, run_id_factory=lambda: next(run_ids))

    first = service.ingest(definition(), spec(csv_path, CalendarSourceType.CSV, source_id="csv"))
    second = service.ingest(
        definition(),
        spec(parquet_path, CalendarSourceType.PARQUET, source_id="parquet"),
    )

    assert first.snapshot_id == second.snapshot_id
    assert first.content_hash == second.content_hash
    assert second.reused_existing is True


def test_closed_day_commits_without_sessions(tmp_path):
    path = tmp_path / "closed.csv"
    write_csv(
        path,
        [
            {
                "calendar_date": "2026-07-07",
                "is_trading_day": False,
                "session_id": "",
                "session_start": "",
                "session_end": "",
                "session_kind": "",
            }
        ],
    )
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")
    result = CalendarIngestionService(store, run_id_factory=lambda: "closed-run").ingest(
        definition(), spec(path, CalendarSourceType.CSV)
    )

    assert result.status == CalendarImportStatus.COMMITTED
    assert store.get_snapshot(result.snapshot_id).sessions == ()


def test_closed_day_row_date_mismatch_is_rejected(tmp_path):
    path = tmp_path / "wrong-date.csv"
    write_csv(
        path,
        [
            {
                "calendar_date": "2026-07-08",
                "is_trading_day": False,
                "session_id": "",
                "session_start": "",
                "session_end": "",
                "session_kind": "",
            }
        ],
    )
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")

    result = CalendarIngestionService(store, run_id_factory=lambda: "wrong-date-run").ingest(
        definition(), spec(path, CalendarSourceType.CSV)
    )

    assert result.status == CalendarImportStatus.FAILED
    assert {item.issue_code for item in result.quality_report.issues} == {
        "PARTITION_DATE_MISMATCH"
    }


def test_overlapping_sessions_fail_and_run_is_retained(tmp_path):
    path = tmp_path / "overlap.csv"
    values = rows()
    values[1]["session_start"] = "11:00:00"
    write_csv(path, values)
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")
    result = CalendarIngestionService(store, run_id_factory=lambda: "overlap-run").ingest(
        definition(), spec(path, CalendarSourceType.CSV)
    )

    assert result.status == CalendarImportStatus.FAILED
    assert result.error_code == "QUALITY_GATE_FAILED"
    assert store.get_import_run("overlap-run").status == CalendarImportStatus.FAILED
    assert {item.issue_code for item in store.list_quality_issues("overlap-run")} == {
        "OVERLAPPING_SESSIONS"
    }


def test_definition_and_historical_partition_conflicts_are_rejected(tmp_path):
    original = tmp_path / "original.csv"
    changed = tmp_path / "changed.csv"
    write_csv(original, rows())
    write_csv(changed, rows(afternoon_end="14:59:00"))
    store = LocalDuckDBCalendarStore(tmp_path / "research.duckdb")
    run_ids = iter(["first", "changed"])
    service = CalendarIngestionService(store, run_id_factory=lambda: next(run_ids))
    first = service.ingest(definition(), spec(original, CalendarSourceType.CSV, source_id="first"))
    conflict = service.ingest(
        definition(), spec(changed, CalendarSourceType.CSV, source_id="changed")
    )

    assert first.status == CalendarImportStatus.COMMITTED
    assert conflict.error_code == "IMMUTABLE_PARTITION_CONFLICT"
    assert store.get_snapshot(first.snapshot_id).content_hash == first.content_hash
    with pytest.raises(CalendarStoreError) as exc_info:
        store.register_definition(replace(definition(), name="changed"))
    assert exc_info.value.code == "DEFINITION_CONFLICT"
