from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from quant_research.contracts.bar import Adjustment, Frequency


class ImportStatus(StrEnum):
    CREATED = "CREATED"
    READING = "READING"
    NORMALIZING = "NORMALIZING"
    VALIDATING = "VALIDATING"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ImportRun:
    import_run_id: str
    dataset_id: str
    source_id: str
    freq: Frequency
    adjustment: Adjustment
    source_file_hash: str
    status: ImportStatus
    started_at: datetime
    finished_at: datetime | None = None
    row_count_raw: int = 0
    row_count_curated: int = 0
    issue_count: int = 0
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def create(
        cls,
        *,
        import_run_id: str,
        dataset_id: str,
        source_id: str,
        freq: Frequency,
        adjustment: Adjustment,
        source_file_hash: str,
    ) -> "ImportRun":
        return cls(
            import_run_id=import_run_id,
            dataset_id=dataset_id,
            source_id=source_id,
            freq=freq,
            adjustment=adjustment,
            source_file_hash=source_file_hash,
            status=ImportStatus.CREATED,
            started_at=datetime.now(UTC),
        )

