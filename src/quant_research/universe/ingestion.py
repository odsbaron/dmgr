from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from uuid import uuid4

from quant_research.universe.contracts import (
    UniverseDefinition,
    UniverseImportRun,
    UniverseImportStatus,
    UniverseSnapshot,
    UniverseSourceSpec,
    UniverseSourceType,
    hash_file,
)
from quant_research.universe.duckdb_store import LocalDuckDBUniverseStore, UniverseStoreError
from quant_research.universe.normalize import normalize_universe_rows
from quant_research.universe.quality import UniverseQualityReport, UniverseQualityValidator
from quant_research.universe.readers.base import UniverseReader
from quant_research.universe.readers.csv_reader import CSVUniverseReader
from quant_research.universe.readers.parquet_reader import ParquetUniverseReader


@dataclass(frozen=True)
class UniverseIngestionResult:
    import_run_id: str
    status: UniverseImportStatus
    snapshot_id: str | None
    content_hash: str | None
    row_count_raw: int
    row_count_member: int
    quality_report: UniverseQualityReport
    source_file_hash: str
    reused_existing: bool = False
    error_code: str | None = None
    error_message: str | None = None


class UniverseIngestionService:
    def __init__(
        self,
        store: LocalDuckDBUniverseStore,
        *,
        readers: Mapping[UniverseSourceType, UniverseReader] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ):
        self.store = store
        self._readers = readers or {
            UniverseSourceType.CSV: CSVUniverseReader(),
            UniverseSourceType.PARQUET: ParquetUniverseReader(),
        }
        self._run_id_factory = run_id_factory or (lambda: str(uuid4()))

    def ingest(
        self,
        definition: UniverseDefinition,
        spec: UniverseSourceSpec,
    ) -> UniverseIngestionResult:
        self.store.register_definition(definition)
        source_file_hash = hash_file(spec.path)
        run = UniverseImportRun.create(
            import_run_id=self._run_id_factory(),
            spec=spec,
            source_file_hash=source_file_hash,
            definition_hash=definition.definition_hash,
        )
        existing_run = self.store.find_committed_import(run.import_fingerprint)
        if existing_run is not None and existing_run.snapshot_id is not None:
            snapshot = self.store.get_snapshot(existing_run.snapshot_id)
            return UniverseIngestionResult(
                import_run_id=existing_run.import_run_id,
                status=existing_run.status,
                snapshot_id=existing_run.snapshot_id,
                content_hash=snapshot.content_hash if snapshot else None,
                row_count_raw=existing_run.row_count_raw,
                row_count_member=existing_run.row_count_member,
                quality_report=UniverseQualityReport(existing_run.import_run_id, ()),
                source_file_hash=source_file_hash,
                reused_existing=True,
            )

        try:
            raw_rows = list(self._reader_for(spec.source_type).read_rows(spec))
            run = replace(run, row_count_raw=len(raw_rows))
            members = normalize_universe_rows(raw_rows, spec, import_run_id=run.import_run_id)
        except Exception as exc:
            report = UniverseQualityReport(run.import_run_id, ())
            failed = self.store.fail_import(
                run,
                report,
                error_code="READ_NORMALIZE_FAILED",
                error_message=str(exc),
                row_count_raw=run.row_count_raw,
            )
            return self._failed_result(failed, report)

        report = UniverseQualityValidator(run.import_run_id).validate(definition, spec, members)
        if report.has_blocking_errors and spec.strict_mode:
            failed = self.store.fail_import(
                run,
                report,
                error_code="QUALITY_GATE_FAILED",
                error_message="blocking Universe quality errors",
                row_count_raw=len(raw_rows),
            )
            return self._failed_result(failed, report)

        snapshot = UniverseSnapshot.create(
            definition,
            spec,
            members,
            source_file_hash=source_file_hash,
        )
        try:
            commit = self.store.commit_snapshot(run, snapshot, report)
        except UniverseStoreError as exc:
            failed = self.store.fail_import(
                run,
                report,
                error_code=exc.code,
                error_message=exc.message,
                row_count_raw=len(raw_rows),
            )
            return self._failed_result(failed, report)
        return UniverseIngestionResult(
            import_run_id=run.import_run_id,
            status=UniverseImportStatus.COMMITTED,
            snapshot_id=commit.snapshot.snapshot_id,
            content_hash=commit.snapshot.content_hash,
            row_count_raw=len(raw_rows),
            row_count_member=len(commit.snapshot.members),
            quality_report=report,
            source_file_hash=source_file_hash,
            reused_existing=commit.reused_existing,
        )

    def _reader_for(self, source_type: UniverseSourceType) -> UniverseReader:
        try:
            return self._readers[source_type]
        except KeyError as exc:
            raise ValueError(f"no Universe reader registered for source type: {source_type}") from exc

    def _failed_result(
        self,
        failed: UniverseImportRun,
        report: UniverseQualityReport,
    ) -> UniverseIngestionResult:
        return UniverseIngestionResult(
            import_run_id=failed.import_run_id,
            status=failed.status,
            snapshot_id=None,
            content_hash=None,
            row_count_raw=failed.row_count_raw,
            row_count_member=0,
            quality_report=report,
            source_file_hash=failed.source_file_hash,
            error_code=failed.error_code,
            error_message=failed.error_message,
        )
