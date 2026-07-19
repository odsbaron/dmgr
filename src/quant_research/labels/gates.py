from __future__ import annotations

from dataclasses import dataclass

from quant_research.contracts.refs import DataRef
from quant_research.features.quality import QualityStatus
from quant_research.labels.contracts import LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore


class LabelConsumptionBlocked(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class LabelQualityGate:
    label_store: LocalDuckDBLabelStore

    def assert_labels_consumable(self, label_ref: DataRef | str) -> None:
        data_ref = DataRef.parse(label_ref) if isinstance(label_ref, str) else label_ref
        if data_ref.table != "label_table":
            raise LabelConsumptionBlocked(
                "UNSUPPORTED_REF",
                f"label consumption requires label_table ref, got {data_ref.table}",
            )

        label_run_id = data_ref.filters.get("label_run_id")
        if not label_run_id:
            raise LabelConsumptionBlocked(
                "MISSING_LABEL_RUN_ID",
                "label_table ref requires label_run_id for quality gating",
            )

        manifest = self.label_store.get_manifest(label_run_id)
        if manifest is None:
            raise LabelConsumptionBlocked(
                "MISSING_MANIFEST",
                f"missing label_run_manifest for label_run_id={label_run_id}",
            )
        if manifest.status != "COMMITTED":
            raise LabelConsumptionBlocked(
                "RUN_NOT_COMMITTED",
                f"label run is not committed: {manifest.status}",
            )
        if manifest.quality_status != QualityStatus.PASSED.value:
            raise LabelConsumptionBlocked(
                "QUALITY_NOT_PASSED",
                f"label table quality is not PASSED: {manifest.quality_status}",
            )

    def read_consumable_labels(self, label_ref: DataRef | str) -> list[LabelValue]:
        self.assert_labels_consumable(label_ref)
        return self.label_store.read_labels(label_ref)
