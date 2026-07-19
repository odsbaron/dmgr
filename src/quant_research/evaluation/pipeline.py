from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from quant_research import __version__
from quant_research.contracts.refs import DataRef
from quant_research.evaluation.analytics import evaluate_cross_sections
from quant_research.evaluation.contracts import (
    EvaluationRunStatus,
    FactorEvaluationError,
    FactorEvaluationManifest,
    FactorEvaluationRequest,
    FactorEvaluationResult,
    canonical_data_ref,
    evaluation_config_hash,
    evaluation_content_hash,
    evaluation_manifest_ref,
    evaluation_metric_ref,
)
from quant_research.evaluation.duckdb_store import LocalDuckDBEvaluationStore
from quant_research.features.contracts import FeatureRunManifest
from quant_research.features.gates import FeatureQualityGate
from quant_research.labels.contracts import LabelRunManifest
from quant_research.labels.gates import LabelQualityGate


@dataclass(frozen=True)
class FactorEvaluationPipeline:
    feature_gate: FeatureQualityGate
    label_gate: LabelQualityGate
    evaluation_store: LocalDuckDBEvaluationStore

    def run(self, request: FactorEvaluationRequest) -> FactorEvaluationResult:
        snapshots = self.feature_gate.read_consumable_snapshot(request.feature_snapshot_ref)
        labels = self.label_gate.read_consumable_labels(request.label_ref)
        feature_manifest, label_manifest = self._input_manifests(request)
        _assert_compatible(feature_manifest, label_manifest)

        computation = evaluate_cross_sections(request, snapshots, labels)
        metric_ref = evaluation_metric_ref(request.evaluation_run_id)
        manifest = FactorEvaluationManifest(
            evaluation_run_id=request.evaluation_run_id,
            feature_snapshot_ref=canonical_data_ref(request.feature_snapshot_ref),
            label_ref=canonical_data_ref(request.label_ref),
            factor_run_id=feature_manifest.factor_run_id,
            label_run_id=label_manifest.label_run_id,
            dataset_id=feature_manifest.dataset_id,
            freq=feature_manifest.freq,
            factor_fields=request.factor_fields,
            label_field=request.label_field,
            quantile_count=request.quantile_count,
            minimum_cross_section_size=request.minimum_cross_section_size,
            long_short_direction=request.long_short_direction,
            row_count_aligned=computation.row_count_aligned,
            evaluated_cross_section_count=computation.evaluated_cross_section_count,
            skipped_cross_section_count=computation.skipped_cross_section_count,
            metric_count=len(computation.metrics),
            status=EvaluationRunStatus.COMMITTED,
            created_at=datetime.now(UTC).isoformat(),
            code_version=__version__,
            config_hash=evaluation_config_hash(request),
            content_hash=evaluation_content_hash(computation.metrics),
            metric_ref=metric_ref.uri,
        )
        commit = self.evaluation_store.commit_run(manifest, computation.metrics)
        committed = commit.manifest
        return FactorEvaluationResult(
            evaluation_run_id=committed.evaluation_run_id,
            status=committed.status,
            manifest_ref=evaluation_manifest_ref(committed.evaluation_run_id),
            metric_ref=DataRef.parse(committed.metric_ref),
            row_count_aligned=committed.row_count_aligned,
            evaluated_cross_section_count=committed.evaluated_cross_section_count,
            skipped_cross_section_count=committed.skipped_cross_section_count,
            metric_count=committed.metric_count,
            reused_existing=commit.reused_existing,
        )

    def _input_manifests(
        self,
        request: FactorEvaluationRequest,
    ) -> tuple[FeatureRunManifest, LabelRunManifest]:
        feature_ref = DataRef.parse(request.feature_snapshot_ref)
        label_ref = DataRef.parse(request.label_ref)
        factor_run_id = feature_ref.filters.get("factor_run_id", "")
        label_run_id = label_ref.filters.get("label_run_id", "")
        feature_manifest = self.feature_gate.feature_store.get_manifest(factor_run_id)
        label_manifest = self.label_gate.label_store.get_manifest(label_run_id)
        if feature_manifest is None or label_manifest is None:
            raise FactorEvaluationError(
                "MISSING_INPUT_MANIFEST",
                "feature and label manifests are required for factor evaluation",
            )
        return feature_manifest, label_manifest


def _assert_compatible(
    feature_manifest: FeatureRunManifest,
    label_manifest: LabelRunManifest,
) -> None:
    if (
        label_manifest.dataset_id is not None
        and feature_manifest.dataset_id != label_manifest.dataset_id
    ):
        raise FactorEvaluationError(
            "DATASET_ID_MISMATCH",
            "feature and label dataset ids differ",
        )
    if label_manifest.freq is not None and feature_manifest.freq != label_manifest.freq:
        raise FactorEvaluationError(
            "FREQUENCY_MISMATCH",
            "feature and label frequencies differ",
        )
