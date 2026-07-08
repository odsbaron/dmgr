from __future__ import annotations

from dataclasses import dataclass

from quant_research.contracts.refs import DataRef
from quant_research.features.contracts import FeatureRunStatus, FeatureSnapshot
from quant_research.features.duckdb_store import LocalDuckDBFeatureStore
from quant_research.features.quality import QualityStatus


class FeatureConsumptionBlocked(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class FeatureQualityGate:
    feature_store: LocalDuckDBFeatureStore

    def assert_snapshot_consumable(self, snapshot_ref: DataRef | str) -> None:
        data_ref = DataRef.parse(snapshot_ref) if isinstance(snapshot_ref, str) else snapshot_ref
        if data_ref.table != "feature_snapshot":
            raise FeatureConsumptionBlocked(
                "UNSUPPORTED_REF",
                f"feature consumption requires feature_snapshot ref, got {data_ref.table}",
            )

        factor_run_id = data_ref.filters.get("factor_run_id")
        if not factor_run_id:
            raise FeatureConsumptionBlocked(
                "MISSING_FACTOR_RUN_ID",
                "feature_snapshot ref requires factor_run_id for quality gating",
            )

        manifest = self.feature_store.get_manifest(factor_run_id)
        if manifest is None:
            raise FeatureConsumptionBlocked(
                "MISSING_MANIFEST",
                f"missing factor_run_manifest for factor_run_id={factor_run_id}",
            )
        if manifest.status != FeatureRunStatus.COMMITTED:
            raise FeatureConsumptionBlocked(
                "RUN_NOT_COMMITTED",
                f"factor run is not committed: {manifest.status.value}",
            )
        if manifest.quality_status != QualityStatus.PASSED.value:
            raise FeatureConsumptionBlocked(
                "QUALITY_NOT_PASSED",
                f"feature snapshot quality is not PASSED: {manifest.quality_status}",
            )

    def read_consumable_snapshot(self, snapshot_ref: DataRef | str) -> list[FeatureSnapshot]:
        self.assert_snapshot_consumable(snapshot_ref)
        return self.feature_store.read_snapshot(snapshot_ref)
