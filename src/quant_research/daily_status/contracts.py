from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from quant_research.contracts.bar import AssetClass
from quant_research.temporal_assets import canonical_datetime, canonical_hash, required_text


DAILY_STATUS_SCHEMA_VERSION = "1"


class StatusSourceType(StrEnum):
    CSV = "CSV"
    PARQUET = "PARQUET"


class StatusImportStatus(StrEnum):
    CREATED = "CREATED"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class MarketState(StrEnum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    NOT_LISTED = "NOT_LISTED"
    UNKNOWN = "UNKNOWN"


class BarExpectation(StrEnum):
    FULL_SESSION = "FULL_SESSION"
    NO_BARS = "NO_BARS"
    CUSTOM_INTERVALS = "CUSTOM_INTERVALS"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class LocalTimeInterval:
    start_time: time
    end_time: time

    def __post_init__(self) -> None:
        if self.start_time.tzinfo is not None or self.end_time.tzinfo is not None:
            raise ValueError("expected intervals must use local wall-clock times")

    @property
    def canonical_payload(self) -> dict[str, str]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
        }


@dataclass(frozen=True)
class DailyStatusDefinition:
    status_id: str
    version: str
    name: str
    asset_class: AssetClass
    calendar_id: str
    calendar_version: str
    timezone: str

    def __post_init__(self) -> None:
        for field_name in (
            "status_id",
            "version",
            "name",
            "calendar_id",
            "calendar_version",
            "timezone",
        ):
            required_text(getattr(self, field_name), field_name)
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc

    @property
    def key(self) -> tuple[str, str]:
        return self.status_id, self.version

    @property
    def definition_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": DAILY_STATUS_SCHEMA_VERSION,
                "status_id": self.status_id,
                "version": self.version,
                "name": self.name,
                "asset_class": self.asset_class.value,
                "calendar_id": self.calendar_id,
                "calendar_version": self.calendar_version,
                "timezone": self.timezone,
            }
        )


@dataclass(frozen=True)
class DailyStatusSourceSpec:
    source_id: str
    status_id: str
    status_version: str
    source_type: StatusSourceType
    path: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    field_mapping: Mapping[str, str]
    strict_mode: bool = True

    def __post_init__(self) -> None:
        for field_name in ("source_id", "status_id", "status_version", "path"):
            required_text(getattr(self, field_name), field_name)
        required = {"instrument_id", "market_state", "bar_expectation"}
        missing = sorted(item for item in required if not self.field_mapping.get(item))
        if missing:
            raise ValueError(f"field_mapping is missing required fields: {', '.join(missing)}")

    @property
    def spec_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": DAILY_STATUS_SCHEMA_VERSION,
                "source_id": self.source_id,
                "status_id": self.status_id,
                "status_version": self.status_version,
                "source_type": self.source_type.value,
                "trading_date": self.trading_date.isoformat(),
                "known_at": canonical_datetime(self.known_at),
                "source_data_cutoff": canonical_datetime(self.source_data_cutoff),
                "field_mapping": dict(sorted(self.field_mapping.items())),
                "strict_mode": self.strict_mode,
            }
        )


@dataclass(frozen=True)
class InstrumentDailyStatus:
    instrument_id: str
    market_state: MarketState
    bar_expectation: BarExpectation
    custom_intervals: tuple[LocalTimeInterval, ...] = ()
    declared_trading_date: date | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None

    @property
    def canonical_payload(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "market_state": self.market_state.value,
            "bar_expectation": self.bar_expectation.value,
            "custom_intervals": [item.canonical_payload for item in self.custom_intervals],
        }


@dataclass(frozen=True)
class DailyStatusSnapshot:
    snapshot_id: str
    status_id: str
    status_version: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    definition_hash: str
    content_hash: str
    source_ref: str
    source_file_hash: str
    statuses: tuple[InstrumentDailyStatus, ...]

    @classmethod
    def create(
        cls,
        definition: DailyStatusDefinition,
        spec: DailyStatusSourceSpec,
        statuses: tuple[InstrumentDailyStatus, ...],
        *,
        source_file_hash: str,
    ) -> "DailyStatusSnapshot":
        ordered = tuple(sorted(statuses, key=lambda item: item.instrument_id))
        content_hash = canonical_hash(
            {
                "schema_version": DAILY_STATUS_SCHEMA_VERSION,
                "definition_hash": definition.definition_hash,
                "trading_date": spec.trading_date.isoformat(),
                "known_at": canonical_datetime(spec.known_at),
                "source_data_cutoff": canonical_datetime(spec.source_data_cutoff),
                "statuses": [item.canonical_payload for item in ordered],
            }
        )
        return cls(
            snapshot_id=f"daily-status-{content_hash.removeprefix('sha256:')[:24]}",
            status_id=definition.status_id,
            status_version=definition.version,
            trading_date=spec.trading_date,
            known_at=spec.known_at,
            source_data_cutoff=spec.source_data_cutoff,
            definition_hash=definition.definition_hash,
            content_hash=content_hash,
            source_ref=str(Path(spec.path)),
            source_file_hash=source_file_hash,
            statuses=ordered,
        )


@dataclass(frozen=True)
class StatusImportRun:
    import_run_id: str
    source_id: str
    status_id: str
    status_version: str
    trading_date: date
    source_file_hash: str
    import_fingerprint: str
    status: StatusImportStatus
    started_at: datetime
    finished_at: datetime | None = None
    snapshot_id: str | None = None
    row_count_raw: int = 0
    row_count_status: int = 0
    issue_count: int = 0
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def create(
        cls,
        import_run_id: str,
        spec: DailyStatusSourceSpec,
        source_file_hash: str,
        definition_hash: str,
    ) -> "StatusImportRun":
        return cls(
            import_run_id=import_run_id,
            source_id=spec.source_id,
            status_id=spec.status_id,
            status_version=spec.status_version,
            trading_date=spec.trading_date,
            source_file_hash=source_file_hash,
            import_fingerprint=canonical_hash(
                {
                    "schema_version": DAILY_STATUS_SCHEMA_VERSION,
                    "source_file_hash": source_file_hash,
                    "source_spec_hash": spec.spec_hash,
                    "definition_hash": definition_hash,
                }
            ),
            status=StatusImportStatus.CREATED,
            started_at=datetime.now(UTC),
        )


@dataclass(frozen=True)
class StatusSnapshotSetItem:
    trading_date: date
    snapshot_id: str
    content_hash: str


@dataclass(frozen=True)
class StatusSnapshotSet:
    snapshot_set_id: str
    status_id: str
    status_version: str
    definition_hash: str
    date_start: date
    date_end: date
    snapshot_set_hash: str
    items: tuple[StatusSnapshotSetItem, ...]
    created_at: datetime

    @classmethod
    def create(
        cls,
        status_id: str,
        status_version: str,
        definition_hash: str,
        items: tuple[StatusSnapshotSetItem, ...],
    ) -> "StatusSnapshotSet":
        if not items:
            raise ValueError("snapshot set items must not be empty")
        ordered = tuple(sorted(items, key=lambda item: item.trading_date))
        set_hash = canonical_hash(
            {
                "schema_version": DAILY_STATUS_SCHEMA_VERSION,
                "status_id": status_id,
                "status_version": status_version,
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
            snapshot_set_id=f"daily-status-set-{set_hash.removeprefix('sha256:')[:24]}",
            status_id=status_id,
            status_version=status_version,
            definition_hash=definition_hash,
            date_start=ordered[0].trading_date,
            date_end=ordered[-1].trading_date,
            snapshot_set_hash=set_hash,
            items=ordered,
            created_at=datetime.now(UTC),
        )

    @property
    def ref(self) -> "DailyStatusRef":
        return DailyStatusRef(self.snapshot_set_id)


@dataclass(frozen=True)
class DailyStatusRef:
    snapshot_set_id: str

    def __post_init__(self) -> None:
        required_text(self.snapshot_set_id, "snapshot_set_id")

    @property
    def uri(self) -> str:
        return f"duckdb://instrument_daily_status?{urlencode({'snapshot_set_id': self.snapshot_set_id})}"

    @classmethod
    def parse(cls, value: "DailyStatusRef | str") -> "DailyStatusRef":
        if isinstance(value, cls):
            return value
        parsed = urlparse(value)
        table = parsed.netloc or parsed.path.lstrip("/")
        filters = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if parsed.scheme != "duckdb" or table != "instrument_daily_status":
            raise ValueError("status_ref must point to duckdb://instrument_daily_status")
        if set(filters) != {"snapshot_set_id"} or not filters["snapshot_set_id"]:
            raise ValueError("status_ref requires only snapshot_set_id")
        return cls(filters["snapshot_set_id"])

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class ResolvedDailyStatus:
    status_ref: DailyStatusRef
    status_id: str
    status_version: str
    asset_class: AssetClass
    calendar_id: str
    calendar_version: str
    timezone: str
    definition_hash: str
    snapshot_set_hash: str
    statuses_by_date: Mapping[date, Mapping[str, InstrumentDailyStatus]]

    @property
    def trading_dates(self) -> tuple[date, ...]:
        return tuple(sorted(self.statuses_by_date))
