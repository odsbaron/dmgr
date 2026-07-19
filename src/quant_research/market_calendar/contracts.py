from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from quant_research.temporal_assets import canonical_datetime, canonical_hash, required_text


CALENDAR_SCHEMA_VERSION = "1"


class CalendarSourceType(StrEnum):
    CSV = "CSV"
    PARQUET = "PARQUET"


class CalendarImportStatus(StrEnum):
    CREATED = "CREATED"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class MarketCalendarDefinition:
    calendar_id: str
    version: str
    name: str
    timezone: str

    def __post_init__(self) -> None:
        for field_name in ("calendar_id", "version", "name", "timezone"):
            required_text(getattr(self, field_name), field_name)
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc

    @property
    def key(self) -> tuple[str, str]:
        return self.calendar_id, self.version

    @property
    def definition_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": CALENDAR_SCHEMA_VERSION,
                "calendar_id": self.calendar_id,
                "version": self.version,
                "name": self.name,
                "timezone": self.timezone,
            }
        )


@dataclass(frozen=True)
class CalendarSourceSpec:
    source_id: str
    calendar_id: str
    calendar_version: str
    source_type: CalendarSourceType
    path: str
    calendar_date: date
    known_at: datetime
    source_data_cutoff: datetime
    field_mapping: Mapping[str, str]
    strict_mode: bool = True

    def __post_init__(self) -> None:
        for field_name in ("source_id", "calendar_id", "calendar_version", "path"):
            required_text(getattr(self, field_name), field_name)
        if not self.field_mapping.get("is_trading_day"):
            raise ValueError("field_mapping requires is_trading_day")

    @property
    def spec_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": CALENDAR_SCHEMA_VERSION,
                "source_id": self.source_id,
                "calendar_id": self.calendar_id,
                "calendar_version": self.calendar_version,
                "source_type": self.source_type.value,
                "calendar_date": self.calendar_date.isoformat(),
                "known_at": canonical_datetime(self.known_at),
                "source_data_cutoff": canonical_datetime(self.source_data_cutoff),
                "field_mapping": dict(sorted(self.field_mapping.items())),
                "strict_mode": self.strict_mode,
            }
        )


@dataclass(frozen=True)
class MarketSession:
    session_id: str
    start_time: time | None
    end_time: time | None
    session_kind: str = "REGULAR"
    declared_calendar_date: date | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None

    @property
    def canonical_payload(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "session_kind": self.session_kind,
        }


@dataclass(frozen=True)
class NormalizedCalendarDay:
    is_trading_day: bool
    declared_states: tuple[bool, ...]
    sessions: tuple[MarketSession, ...]
    row_count: int
    declared_dates: tuple[date | None, ...] = ()


@dataclass(frozen=True)
class CalendarDaySnapshot:
    snapshot_id: str
    calendar_id: str
    calendar_version: str
    calendar_date: date
    is_trading_day: bool
    known_at: datetime
    source_data_cutoff: datetime
    definition_hash: str
    content_hash: str
    source_ref: str
    source_file_hash: str
    sessions: tuple[MarketSession, ...]

    @classmethod
    def create(
        cls,
        definition: MarketCalendarDefinition,
        spec: CalendarSourceSpec,
        day: NormalizedCalendarDay,
        *,
        source_file_hash: str,
    ) -> "CalendarDaySnapshot":
        ordered = tuple(
            sorted(
                day.sessions,
                key=lambda item: (item.start_time or time.min, item.session_id),
            )
        )
        content_hash = canonical_hash(
            {
                "schema_version": CALENDAR_SCHEMA_VERSION,
                "definition_hash": definition.definition_hash,
                "calendar_date": spec.calendar_date.isoformat(),
                "is_trading_day": day.is_trading_day,
                "known_at": canonical_datetime(spec.known_at),
                "source_data_cutoff": canonical_datetime(spec.source_data_cutoff),
                "sessions": [session.canonical_payload for session in ordered],
            }
        )
        return cls(
            snapshot_id=f"calendar-day-{content_hash.removeprefix('sha256:')[:24]}",
            calendar_id=definition.calendar_id,
            calendar_version=definition.version,
            calendar_date=spec.calendar_date,
            is_trading_day=day.is_trading_day,
            known_at=spec.known_at,
            source_data_cutoff=spec.source_data_cutoff,
            definition_hash=definition.definition_hash,
            content_hash=content_hash,
            source_ref=str(Path(spec.path)),
            source_file_hash=source_file_hash,
            sessions=ordered,
        )


@dataclass(frozen=True)
class CalendarImportRun:
    import_run_id: str
    source_id: str
    calendar_id: str
    calendar_version: str
    calendar_date: date
    source_file_hash: str
    import_fingerprint: str
    status: CalendarImportStatus
    started_at: datetime
    finished_at: datetime | None = None
    snapshot_id: str | None = None
    row_count_raw: int = 0
    session_count: int = 0
    issue_count: int = 0
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def create(
        cls,
        import_run_id: str,
        spec: CalendarSourceSpec,
        source_file_hash: str,
        definition_hash: str,
    ) -> "CalendarImportRun":
        return cls(
            import_run_id=import_run_id,
            source_id=spec.source_id,
            calendar_id=spec.calendar_id,
            calendar_version=spec.calendar_version,
            calendar_date=spec.calendar_date,
            source_file_hash=source_file_hash,
            import_fingerprint=canonical_hash(
                {
                    "schema_version": CALENDAR_SCHEMA_VERSION,
                    "source_file_hash": source_file_hash,
                    "source_spec_hash": spec.spec_hash,
                    "definition_hash": definition_hash,
                }
            ),
            status=CalendarImportStatus.CREATED,
            started_at=datetime.now(UTC),
        )


@dataclass(frozen=True)
class CalendarSnapshotSetItem:
    calendar_date: date
    snapshot_id: str
    content_hash: str


@dataclass(frozen=True)
class CalendarSnapshotSet:
    snapshot_set_id: str
    calendar_id: str
    calendar_version: str
    definition_hash: str
    date_start: date
    date_end: date
    snapshot_set_hash: str
    items: tuple[CalendarSnapshotSetItem, ...]
    created_at: datetime

    @classmethod
    def create(
        cls,
        calendar_id: str,
        calendar_version: str,
        definition_hash: str,
        items: tuple[CalendarSnapshotSetItem, ...],
    ) -> "CalendarSnapshotSet":
        if not items:
            raise ValueError("snapshot set items must not be empty")
        ordered = tuple(sorted(items, key=lambda item: item.calendar_date))
        set_hash = canonical_hash(
            {
                "schema_version": CALENDAR_SCHEMA_VERSION,
                "calendar_id": calendar_id,
                "calendar_version": calendar_version,
                "definition_hash": definition_hash,
                "items": [
                    {
                        "calendar_date": item.calendar_date.isoformat(),
                        "snapshot_id": item.snapshot_id,
                        "content_hash": item.content_hash,
                    }
                    for item in ordered
                ],
            }
        )
        return cls(
            snapshot_set_id=f"calendar-set-{set_hash.removeprefix('sha256:')[:24]}",
            calendar_id=calendar_id,
            calendar_version=calendar_version,
            definition_hash=definition_hash,
            date_start=ordered[0].calendar_date,
            date_end=ordered[-1].calendar_date,
            snapshot_set_hash=set_hash,
            items=ordered,
            created_at=datetime.now(UTC),
        )

    @property
    def ref(self) -> "CalendarRef":
        return CalendarRef(self.snapshot_set_id)


@dataclass(frozen=True)
class CalendarRef:
    snapshot_set_id: str

    def __post_init__(self) -> None:
        required_text(self.snapshot_set_id, "snapshot_set_id")

    @property
    def uri(self) -> str:
        return f"duckdb://market_calendar_day?{urlencode({'snapshot_set_id': self.snapshot_set_id})}"

    @classmethod
    def parse(cls, value: "CalendarRef | str") -> "CalendarRef":
        if isinstance(value, cls):
            return value
        parsed = urlparse(value)
        table = parsed.netloc or parsed.path.lstrip("/")
        filters = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if parsed.scheme != "duckdb" or table != "market_calendar_day":
            raise ValueError("calendar_ref must point to duckdb://market_calendar_day")
        if set(filters) != {"snapshot_set_id"} or not filters["snapshot_set_id"]:
            raise ValueError("calendar_ref requires only snapshot_set_id")
        return cls(filters["snapshot_set_id"])

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class ResolvedMarketCalendar:
    calendar_ref: CalendarRef
    calendar_id: str
    calendar_version: str
    timezone: str
    definition_hash: str
    snapshot_set_hash: str
    days_by_date: Mapping[date, CalendarDaySnapshot]

    @property
    def calendar_dates(self) -> tuple[date, ...]:
        return tuple(sorted(self.days_by_date))
