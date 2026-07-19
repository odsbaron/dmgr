from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from uuid import uuid4

from quant_research.daily_status.contracts import (
    DailyStatusDefinition,
    DailyStatusSnapshot,
    DailyStatusSourceSpec,
    StatusImportRun,
    StatusImportStatus,
    StatusSourceType,
)
from quant_research.daily_status.duckdb_store import (
    DailyStatusStoreError,
    LocalDuckDBDailyStatusStore,
)
from quant_research.daily_status.io import (
    CSVStatusReader,
    DailyStatusReader,
    ParquetStatusReader,
    normalize_status_rows,
)
from quant_research.daily_status.quality import StatusQualityReport, StatusQualityValidator
from quant_research.temporal_assets import hash_file


@dataclass(frozen=True)
class DailyStatusIngestionResult:
    import_run_id: str
    status: StatusImportStatus
    snapshot_id: str | None
    content_hash: str | None
    row_count_raw: int
    row_count_status: int
    quality_report: StatusQualityReport
    source_file_hash: str
    reused_existing: bool = False
    error_code: str | None = None
    error_message: str | None = None


class DailyStatusIngestionService:
    def __init__(
        self,
        store: LocalDuckDBDailyStatusStore,
        *,
        readers: Mapping[StatusSourceType, DailyStatusReader] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ):
        self.store = store
        self.readers = readers or {
            StatusSourceType.CSV: CSVStatusReader(),
            StatusSourceType.PARQUET: ParquetStatusReader(),
        }
        self.run_id_factory = run_id_factory or (lambda: str(uuid4()))

    def ingest(
        self,
        definition: DailyStatusDefinition,
        spec: DailyStatusSourceSpec,
    ) -> DailyStatusIngestionResult:
        self.store.register_definition(definition)
        source_hash = hash_file(spec.path)
        run = StatusImportRun.create(
            self.run_id_factory(), spec, source_hash, definition.definition_hash
        )
        replay = self.store.find_committed_import(run.import_fingerprint)
        if replay is not None and replay.snapshot_id:
            snapshot = self.store.get_snapshot(replay.snapshot_id)
            return DailyStatusIngestionResult(
                replay.import_run_id, replay.status, replay.snapshot_id,
                snapshot.content_hash if snapshot else None, replay.row_count_raw,
                replay.row_count_status, StatusQualityReport(replay.import_run_id, ()),
                source_hash, reused_existing=True,
            )
        try:
            rows = list(self.readers[spec.source_type].read_rows(spec))
            run = replace(run, row_count_raw=len(rows))
            statuses = normalize_status_rows(rows, spec, import_run_id=run.import_run_id)
        except Exception as exc:
            report = StatusQualityReport(run.import_run_id, ())
            failed = self.store.fail_import(
                run, report, error_code="READ_NORMALIZE_FAILED",
                error_message=str(exc), row_count_raw=run.row_count_raw,
            )
            return self._failed(failed, report)
        report = StatusQualityValidator(run.import_run_id).validate(definition, spec, statuses)
        if report.has_blocking_errors and spec.strict_mode:
            failed = self.store.fail_import(
                run, report, error_code="QUALITY_GATE_FAILED",
                error_message="blocking daily status quality errors", row_count_raw=len(rows),
            )
            return self._failed(failed, report)
        snapshot = DailyStatusSnapshot.create(
            definition, spec, statuses, source_file_hash=source_hash
        )
        try:
            commit = self.store.commit_snapshot(run, snapshot, report)
        except DailyStatusStoreError as exc:
            failed = self.store.fail_import(
                run, report, error_code=exc.code, error_message=exc.message,
                row_count_raw=len(rows),
            )
            return self._failed(failed, report)
        return DailyStatusIngestionResult(
            run.import_run_id, StatusImportStatus.COMMITTED, commit.snapshot.snapshot_id,
            commit.snapshot.content_hash, len(rows), len(commit.snapshot.statuses), report,
            source_hash, reused_existing=commit.reused_existing,
        )

    def _failed(
        self,
        run: StatusImportRun,
        report: StatusQualityReport,
    ) -> DailyStatusIngestionResult:
        return DailyStatusIngestionResult(
            run.import_run_id, run.status, None, None, run.row_count_raw, 0,
            report, run.source_file_hash, error_code=run.error_code,
            error_message=run.error_message,
        )
