from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum


class TrainingDatasetStatus(StrEnum):
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
