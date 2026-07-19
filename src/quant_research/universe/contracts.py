from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from quant_research.contracts.bar import AssetClass


UNIVERSE_SCHEMA_VERSION = "1"


class UniverseConstructionMode(StrEnum):
    IMPORTED_SNAPSHOT = "IMPORTED_SNAPSHOT"
    RULE_BASED = "RULE_BASED"


class UniverseSourceType(StrEnum):
    CSV = "CSV"
    PARQUET = "PARQUET"


class UniverseImportStatus(StrEnum):
    CREATED = "CREATED"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


def canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def hash_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.isoformat()
    return value.astimezone(UTC).isoformat()


def _required_text(value: str, field_name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")


@dataclass(frozen=True)
class UniverseDefinition:
    universe_id: str
    version: str
    name: str
    asset_class: AssetClass
    calendar_id: str
    timezone: str
    selection_cutoff_time: time
    construction_mode: UniverseConstructionMode = UniverseConstructionMode.IMPORTED_SNAPSHOT

    def __post_init__(self) -> None:
        for field_name in ("universe_id", "version", "name", "calendar_id", "timezone"):
            _required_text(getattr(self, field_name), field_name)
        if self.selection_cutoff_time.tzinfo is not None:
            raise ValueError("selection_cutoff_time must be a local wall-clock time")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc

    @property
    def key(self) -> tuple[str, str]:
        return self.universe_id, self.version

    @property
    def definition_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": UNIVERSE_SCHEMA_VERSION,
                "universe_id": self.universe_id,
                "version": self.version,
                "name": self.name,
                "asset_class": self.asset_class.value,
                "calendar_id": self.calendar_id,
                "timezone": self.timezone,
                "selection_cutoff_time": self.selection_cutoff_time.isoformat(),
                "construction_mode": self.construction_mode.value,
            }
        )

    def selection_cutoff(self, trading_date: date) -> datetime:
        return datetime.combine(
            trading_date,
            self.selection_cutoff_time,
            tzinfo=ZoneInfo(self.timezone),
        )


@dataclass(frozen=True)
class UniverseSourceSpec:
    source_id: str
    universe_id: str
    universe_version: str
    source_type: UniverseSourceType
    path: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    field_mapping: Mapping[str, str]
    strict_mode: bool = True

    def __post_init__(self) -> None:
        for field_name in ("source_id", "universe_id", "universe_version", "path"):
            _required_text(getattr(self, field_name), field_name)
        if not self.field_mapping.get("instrument_id"):
            raise ValueError("field_mapping requires instrument_id")

    @property
    def spec_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": UNIVERSE_SCHEMA_VERSION,
                "source_id": self.source_id,
                "universe_id": self.universe_id,
                "universe_version": self.universe_version,
                "source_type": self.source_type.value,
                "trading_date": self.trading_date.isoformat(),
                "known_at": _canonical_datetime(self.known_at),
                "source_data_cutoff": _canonical_datetime(self.source_data_cutoff),
                "field_mapping": dict(sorted(self.field_mapping.items())),
                "strict_mode": self.strict_mode,
            }
        )


@dataclass(frozen=True)
class UniverseMember:
    instrument_id: str
    weight: float | None = None
    rank: int | None = None
    inclusion_tags: tuple[str, ...] = ()
    declared_trading_date: date | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None

    @property
    def canonical_payload(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "weight": self.weight,
            "rank": self.rank,
            "inclusion_tags": sorted(self.inclusion_tags),
        }


@dataclass(frozen=True)
class UniverseSnapshot:
    snapshot_id: str
    universe_id: str
    universe_version: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    definition_hash: str
    content_hash: str
    source_ref: str
    source_file_hash: str
    members: tuple[UniverseMember, ...]

    @classmethod
    def create(
        cls,
        definition: UniverseDefinition,
        spec: UniverseSourceSpec,
        members: tuple[UniverseMember, ...],
        *,
        source_file_hash: str,
    ) -> UniverseSnapshot:
        ordered = tuple(sorted(members, key=lambda member: (member.instrument_id, member.source_row_id or "")))
        content_hash = canonical_hash(
            {
                "schema_version": UNIVERSE_SCHEMA_VERSION,
                "definition_hash": definition.definition_hash,
                "trading_date": spec.trading_date.isoformat(),
                "known_at": _canonical_datetime(spec.known_at),
                "source_data_cutoff": _canonical_datetime(spec.source_data_cutoff),
                "members": [member.canonical_payload for member in ordered],
            }
        )
        return cls(
            snapshot_id=f"universe-snapshot-{content_hash.removeprefix('sha256:')[:24]}",
            universe_id=definition.universe_id,
            universe_version=definition.version,
            trading_date=spec.trading_date,
            known_at=spec.known_at,
            source_data_cutoff=spec.source_data_cutoff,
            definition_hash=definition.definition_hash,
            content_hash=content_hash,
            source_ref=str(Path(spec.path)),
            source_file_hash=source_file_hash,
            members=ordered,
        )


@dataclass(frozen=True)
class UniverseImportRun:
    import_run_id: str
    source_id: str
    universe_id: str
    universe_version: str
    trading_date: date
    source_file_hash: str
    import_fingerprint: str
    status: UniverseImportStatus
    started_at: datetime
    finished_at: datetime | None = None
    snapshot_id: str | None = None
    row_count_raw: int = 0
    row_count_member: int = 0
    issue_count: int = 0
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def create(
        cls,
        *,
        import_run_id: str,
        spec: UniverseSourceSpec,
        source_file_hash: str,
        definition_hash: str,
    ) -> UniverseImportRun:
        fingerprint = canonical_hash(
            {
                "schema_version": UNIVERSE_SCHEMA_VERSION,
                "source_file_hash": source_file_hash,
                "source_spec_hash": spec.spec_hash,
                "definition_hash": definition_hash,
            }
        )
        return cls(
            import_run_id=import_run_id,
            source_id=spec.source_id,
            universe_id=spec.universe_id,
            universe_version=spec.universe_version,
            trading_date=spec.trading_date,
            source_file_hash=source_file_hash,
            import_fingerprint=fingerprint,
            status=UniverseImportStatus.CREATED,
            started_at=datetime.now(UTC),
        )


@dataclass(frozen=True)
class UniverseSnapshotSetItem:
    trading_date: date
    snapshot_id: str
    content_hash: str


@dataclass(frozen=True)
class UniverseSnapshotSet:
    snapshot_set_id: str
    universe_id: str
    universe_version: str
    definition_hash: str
    date_start: date
    date_end: date
    snapshot_set_hash: str
    items: tuple[UniverseSnapshotSetItem, ...]
    created_at: datetime

    @classmethod
    def create(
        cls,
        *,
        universe_id: str,
        universe_version: str,
        definition_hash: str,
        items: tuple[UniverseSnapshotSetItem, ...],
        created_at: datetime | None = None,
    ) -> UniverseSnapshotSet:
        if not items:
            raise ValueError("snapshot set items must not be empty")
        ordered = tuple(sorted(items, key=lambda item: item.trading_date))
        snapshot_set_hash = canonical_hash(
            {
                "schema_version": UNIVERSE_SCHEMA_VERSION,
                "universe_id": universe_id,
                "universe_version": universe_version,
                "definition_hash": definition_hash,
                "items": [
                    {
                        "trading_date": item.trading_date.isoformat(),
                        "snapshot_id": item.snapshot_id,
                        "content_hash": item.content_hash,
                    }
                    for item in ordered
                ],
            }
        )
        return cls(
            snapshot_set_id=f"universe-set-{snapshot_set_hash.removeprefix('sha256:')[:24]}",
            universe_id=universe_id,
            universe_version=universe_version,
            definition_hash=definition_hash,
            date_start=ordered[0].trading_date,
            date_end=ordered[-1].trading_date,
            snapshot_set_hash=snapshot_set_hash,
            items=ordered,
            created_at=created_at or datetime.now(UTC),
        )

    @property
    def ref(self) -> UniverseRef:
        return UniverseRef(self.snapshot_set_id)


@dataclass(frozen=True)
class UniverseRef:
    snapshot_set_id: str

    def __post_init__(self) -> None:
        _required_text(self.snapshot_set_id, "snapshot_set_id")

    @property
    def uri(self) -> str:
        return f"duckdb://universe_member?{urlencode({'snapshot_set_id': self.snapshot_set_id})}"

    @classmethod
    def parse(cls, value: str | UniverseRef) -> UniverseRef:
        if isinstance(value, UniverseRef):
            return value
        parsed = urlparse(value)
        table = parsed.netloc or parsed.path.lstrip("/")
        if parsed.scheme != "duckdb" or table != "universe_member":
            raise ValueError("universe_ref must point to duckdb://universe_member")
        filters = dict(parse_qsl(parsed.query, keep_blank_values=True))
        snapshot_set_id = filters.get("snapshot_set_id")
        if not snapshot_set_id:
            raise ValueError("universe_ref requires snapshot_set_id")
        return cls(snapshot_set_id=snapshot_set_id)

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class DailyUniverseMembership:
    trading_date: date
    snapshot_id: str
    instrument_ids: tuple[str, ...]


@dataclass(frozen=True)
class ResolvedUniverse:
    universe_ref: UniverseRef
    universe_id: str
    universe_version: str
    asset_class: AssetClass
    calendar_id: str
    definition_hash: str
    snapshot_set_hash: str
    daily_memberships: tuple[DailyUniverseMembership, ...]

    @property
    def members_by_date(self) -> dict[date, frozenset[str]]:
        return {
            membership.trading_date: frozenset(membership.instrument_ids)
            for membership in self.daily_memberships
        }

    @property
    def instrument_ids(self) -> frozenset[str]:
        return frozenset(
            instrument_id
            for membership in self.daily_memberships
            for instrument_id in membership.instrument_ids
        )
