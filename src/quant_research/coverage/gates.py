from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from quant_research.contracts.refs import DataRef
from quant_research.coverage.contracts import CoverageReportRef, CoverageRunStatus
from quant_research.coverage.duckdb_store import LocalDuckDBCoverageStore


class CoverageGateError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class CoverageGateProtocol(Protocol):
    def assert_report_consumable(self, report_ref: DataRef | str) -> None: ...


@dataclass(frozen=True)
class CoverageConsumptionGate:
    store: LocalDuckDBCoverageStore

    def assert_report_consumable(self, report_ref: DataRef | str) -> None:
        try:
            ref = CoverageReportRef.parse(report_ref)
        except ValueError as exc:
            raise CoverageGateError("INVALID_COVERAGE_REPORT_REF", str(exc)) from exc
        manifest = self.store.get_manifest(ref)
        if manifest is None:
            raise CoverageGateError(
                "UNKNOWN_COVERAGE_REPORT",
                f"coverage report does not exist: {ref.coverage_run_id}",
            )
        if manifest.status != CoverageRunStatus.COMMITTED:
            raise CoverageGateError(
                "COVERAGE_RUN_NOT_COMMITTED",
                f"coverage run is not committed: {ref.coverage_run_id}",
            )
        if not manifest.consumable:
            raise CoverageGateError(
                "COVERAGE_REPORT_NOT_CONSUMABLE",
                f"coverage report is not consumable: {ref.coverage_run_id}",
            )
