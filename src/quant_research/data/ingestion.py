from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from quant_research.contracts.bar import Adjustment, Frequency
from quant_research.contracts.import_run import ImportRun, ImportStatus
from quant_research.contracts.quality import QualityReport
from quant_research.contracts.refs import DataRef
from quant_research.contracts.source import SourceSpec, SourceType
from quant_research.data.normalize import BarNormalizer
from quant_research.data.quality import KLineQualityValidator
from quant_research.data.readers.base import KLineReader
from quant_research.data.readers.csv_reader import CSVKLineReader


class KLineStore(Protocol):
    def commit_import(
        self,
        run: ImportRun,
        bars,
        report: QualityReport,
    ) -> DataRef:
        ...

    def fail_import(
        self,
        run: ImportRun,
        report: QualityReport,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        ...

    def find_committed_import(
        self,
        *,
        dataset_id: str,
        source_id: str,
        freq: Frequency,
        adjustment: Adjustment,
        source_file_hash: str,
    ) -> ImportRun | None:
        ...


@dataclass(frozen=True)
class IngestionResult:
    import_run_id: str
    status: ImportStatus
    data_ref: DataRef | None
    row_count_raw: int
    row_count_curated: int
    quality_report: QualityReport
    source_file_hash: str
    reused_existing: bool = False


class DataIngestionService:
    def __init__(
        self,
        store: KLineStore,
        *,
        readers: Mapping[SourceType, KLineReader] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ):
        self._store = store
        self._readers = readers or {SourceType.CSV: CSVKLineReader()}
        self._run_id_factory = run_id_factory or (lambda: str(uuid4()))

    def ingest(self, spec: SourceSpec) -> IngestionResult:
        source_file_hash = self._hash_source_file(spec.path)
        existing = self._store.find_committed_import(
            dataset_id=spec.dataset_id,
            source_id=spec.source_id,
            freq=spec.freq,
            adjustment=spec.adjustment,
            source_file_hash=source_file_hash,
        )
        if existing is not None:
            return IngestionResult(
                import_run_id=existing.import_run_id,
                status=existing.status,
                data_ref=self._data_ref_for_import(existing),
                row_count_raw=existing.row_count_raw,
                row_count_curated=existing.row_count_curated,
                quality_report=QualityReport(existing.import_run_id, ()),
                source_file_hash=source_file_hash,
                reused_existing=True,
            )

        reader = self._reader_for(spec.source_type)
        run = ImportRun.create(
            import_run_id=self._run_id_factory(),
            dataset_id=spec.dataset_id,
            source_id=spec.source_id,
            freq=spec.freq,
            adjustment=spec.adjustment,
            source_file_hash=source_file_hash,
        )
        raw_rows = list(reader.read_rows(spec))
        run = replace(run, row_count_raw=len(raw_rows))

        normalizer = BarNormalizer(import_run_id=run.import_run_id)
        bars = [normalizer.normalize(row, spec) for row in raw_rows]
        report = KLineQualityValidator(import_run_id=run.import_run_id).validate(bars)

        if report.has_blocking_errors and spec.strict_mode:
            self._store.fail_import(
                run,
                report,
                error_code="QUALITY_GATE_FAILED",
                error_message="blocking quality errors",
            )
            return IngestionResult(
                import_run_id=run.import_run_id,
                status=ImportStatus.FAILED,
                data_ref=None,
                row_count_raw=len(raw_rows),
                row_count_curated=0,
                quality_report=report,
                source_file_hash=source_file_hash,
            )

        data_ref = self._store.commit_import(run, bars, report)
        return IngestionResult(
            import_run_id=run.import_run_id,
            status=ImportStatus.COMMITTED,
            data_ref=data_ref,
            row_count_raw=len(raw_rows),
            row_count_curated=len(bars),
            quality_report=report,
            source_file_hash=source_file_hash,
        )

    def _reader_for(self, source_type: SourceType) -> KLineReader:
        try:
            return self._readers[source_type]
        except KeyError as exc:
            raise ValueError(f"no K-line reader registered for source type: {source_type}") from exc

    def _hash_source_file(self, path: str) -> str:
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"sha256:{digest.hexdigest()}"

    def _data_ref_for_import(self, run: ImportRun) -> DataRef:
        return DataRef(
            "curated_market_bar",
            {
                "dataset_id": run.dataset_id,
                "freq": run.freq.value,
                "adjustment": run.adjustment.value,
                "source_run_id": run.import_run_id,
            },
        )
