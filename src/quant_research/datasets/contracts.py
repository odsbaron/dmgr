from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from urllib.parse import quote, unquote, urlparse


class TrainingDatasetStatus(StrEnum):
    COMMITTED = "COMMITTED"


class MaterializedDatasetStatus(StrEnum):
    COMMITTED = "COMMITTED"


class TrainingDatasetError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def training_dataset_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


@dataclass(frozen=True)
class TrainingDatasetManifest:
    training_dataset_id: str
    feature_ref: str
    label_ref: str
    factor_run_id: str
    label_run_id: str
    feature_set_id: str
    label_set_id: str
    dataset_id: str
    freq: str
    feature_fields: tuple[str, ...]
    label_fields: tuple[str, ...]
    row_count_feature: int
    row_count_label: int
    row_count_joined: int
    row_count_feature_only: int
    row_count_label_only: int
    content_hash: str
    status: TrainingDatasetStatus
    created_at: str
    market_data_definition_hash: str | None = None
    feature_market_data_snapshot_set_hash: str | None = None
    label_market_data_snapshot_set_hash: str | None = None
    universe_ref: str | None = None
    universe_id: str | None = None
    universe_version: str | None = None
    universe_definition_hash: str | None = None
    universe_snapshot_set_hash: str | None = None
    label_source_kind: str = "LEGACY"
    label_source_ref: str | None = None
    label_forward_bars: int | None = None


@dataclass(frozen=True)
class TrainingDatasetCommitResult:
    manifest: TrainingDatasetManifest
    reused_existing: bool = False


@dataclass(frozen=True)
class MaterializeTrainingDatasetRequest:
    materialized_dataset_id: str
    output_dir: str | Path
    drop_incomplete_warmup: bool = True
    drop_null_labels: bool = True

    def __post_init__(self) -> None:
        if not self.materialized_dataset_id:
            raise ValueError("materialized_dataset_id is required")
        if any(part in self.materialized_dataset_id for part in ("/", "\\", "..")):
            raise ValueError("materialized_dataset_id must be a safe path component")

    @property
    def resolved_output_dir(self) -> Path:
        return Path(self.output_dir).expanduser().resolve()


@dataclass(frozen=True)
class MaterializedTrainingDatasetManifest:
    materialized_dataset_id: str
    training_dataset_id: str
    source_content_hash: str
    artifact_ref: str
    artifact_format: str
    feature_fields: tuple[str, ...]
    label_fields: tuple[str, ...]
    schema_fields: tuple[str, ...]
    row_count_input: int
    row_count_materialized: int
    row_count_dropped_warmup: int
    row_count_dropped_null_labels: int
    drop_incomplete_warmup: bool
    drop_null_labels: bool
    definition_hash: str
    schema_hash: str
    content_hash: str
    artifact_hash: str
    status: MaterializedDatasetStatus
    created_at: str

    @property
    def artifact_path(self) -> Path:
        return path_from_parquet_ref(self.artifact_ref)


@dataclass(frozen=True)
class MaterializedDatasetCommitResult:
    manifest: MaterializedTrainingDatasetManifest
    reused_existing: bool = False


@dataclass(frozen=True)
class MaterializedTrainingDatasetResult:
    manifest: MaterializedTrainingDatasetManifest
    reused_existing: bool = False


def parquet_artifact_ref(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve()
    return f"parquet://{quote(str(resolved))}"


def path_from_parquet_ref(ref: str) -> Path:
    parsed = urlparse(ref)
    if parsed.scheme != "parquet":
        raise TrainingDatasetError(
            "UNSUPPORTED_ARTIFACT_REF",
            f"expected parquet artifact ref, got {parsed.scheme or 'no scheme'}",
        )
    if parsed.netloc or parsed.query or parsed.fragment:
        raise TrainingDatasetError(
            "INVALID_ARTIFACT_REF",
            "parquet artifact ref must contain one absolute local path",
        )
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise TrainingDatasetError(
            "INVALID_ARTIFACT_REF",
            "parquet artifact ref path must be absolute",
        )
    return path
