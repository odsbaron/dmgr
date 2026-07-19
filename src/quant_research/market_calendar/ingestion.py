from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from uuid import uuid4

from quant_research.market_calendar.contracts import (
    CalendarDaySnapshot,
    CalendarImportRun,
    CalendarImportStatus,
    CalendarSourceSpec,
    CalendarSourceType,
    MarketCalendarDefinition,
)
from quant_research.market_calendar.duckdb_store import CalendarStoreError, LocalDuckDBCalendarStore
from quant_research.market_calendar.io import (
    CSVCalendarReader,
    CalendarReader,
    ParquetCalendarReader,
    normalize_calendar_rows,
)
from quant_research.market_calendar.quality import CalendarQualityReport, CalendarQualityValidator
from quant_research.temporal_assets import hash_file


@dataclass(frozen=True)
class CalendarIngestionResult:
    import_run_id: str
    status: CalendarImportStatus
    snapshot_id: str | None
    content_hash: str | None
    row_count_raw: int
    session_count: int
    quality_report: CalendarQualityReport
    source_file_hash: str
    reused_existing: bool = False
    error_code: str | None = None
    error_message: str | None = None


class CalendarIngestionService:
    def __init__(
        self,
        store: LocalDuckDBCalendarStore,
        *,
        readers: Mapping[CalendarSourceType, CalendarReader] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ):
        self.store = store
        self.readers = readers or {
            CalendarSourceType.CSV: CSVCalendarReader(),
            CalendarSourceType.PARQUET: ParquetCalendarReader(),
        }
        self.run_id_factory = run_id_factory or (lambda: str(uuid4()))

    def ingest(
        self,
        definition: MarketCalendarDefinition,
        spec: CalendarSourceSpec,
    ) -> CalendarIngestionResult:
        self.store.register_definition(definition)
        source_hash = hash_file(spec.path)
        run = CalendarImportRun.create(
            self.run_id_factory(), spec, source_hash, definition.definition_hash
        )
        replay = self.store.find_committed_import(run.import_fingerprint)
        if replay is not None and replay.snapshot_id:
            snapshot = self.store.get_snapshot(replay.snapshot_id)
            return CalendarIngestionResult(
                replay.import_run_id, replay.status, replay.snapshot_id,
                snapshot.content_hash if snapshot else None, replay.row_count_raw,
                replay.session_count, CalendarQualityReport(replay.import_run_id, ()),
                source_hash, reused_existing=True,
            )
        try:
            rows = list(self.readers[spec.source_type].read_rows(spec))
            run = replace(run, row_count_raw=len(rows))
            day = normalize_calendar_rows(rows, spec, import_run_id=run.import_run_id)
        except Exception as exc:
            report = CalendarQualityReport(run.import_run_id, ())
            failed = self.store.fail_import(
                run, report, error_code="READ_NORMALIZE_FAILED",
                error_message=str(exc), row_count_raw=run.row_count_raw,
            )
            return self._failed(failed, report)
        report = CalendarQualityValidator(run.import_run_id).validate(definition, spec, day)
        if report.has_blocking_errors and spec.strict_mode:
            failed = self.store.fail_import(
                run, report, error_code="QUALITY_GATE_FAILED",
                error_message="blocking calendar quality errors", row_count_raw=len(rows),
            )
            return self._failed(failed, report)
        snapshot = CalendarDaySnapshot.create(
            definition, spec, day, source_file_hash=source_hash
        )
        try:
            commit = self.store.commit_snapshot(run, snapshot, report)
        except CalendarStoreError as exc:
            failed = self.store.fail_import(
                run, report, error_code=exc.code, error_message=exc.message,
                row_count_raw=len(rows),
            )
            return self._failed(failed, report)
        return CalendarIngestionResult(
            run.import_run_id, CalendarImportStatus.COMMITTED, commit.snapshot.snapshot_id,
            commit.snapshot.content_hash, len(rows), len(commit.snapshot.sessions), report,
            source_hash, reused_existing=commit.reused_existing,
        )

    def _failed(
        self,
        run: CalendarImportRun,
        report: CalendarQualityReport,
    ) -> CalendarIngestionResult:
        return CalendarIngestionResult(
            run.import_run_id, run.status, None, None, run.row_count_raw, 0,
            report, run.source_file_hash, error_code=run.error_code,
            error_message=run.error_message,
        )
