from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.contracts.import_run import ImportStatus
from quant_research.contracts.source import (
    BarTimestampConvention,
    SourceSpec,
    SourceType,
)


MARKET_DATA_SCHEMA_VERSION = "1"


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


def _required_text(value: str, field_name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.isoformat()
    return value.astimezone(UTC).isoformat()


def _canonical_decimal(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        decimal = Decimal(value)
    except InvalidOperation:
        return value
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")


@dataclass(frozen=True)
class MarketDatasetDefinition:
    dataset_id: str
    version: str
    name: str
    asset_class: AssetClass
    freq: Frequency
    adjustment: Adjustment
    calendar_id: str
    timezone: str
    bar_timestamp_convention: BarTimestampConvention = BarTimestampConvention.START_TIME

    def __post_init__(self) -> None:
        for field_name in ("dataset_id", "version", "name", "calendar_id", "timezone"):
            _required_text(getattr(self, field_name), field_name)
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc

    @property
    def key(self) -> tuple[str, str]:
        return self.dataset_id, self.version

    @property
    def definition_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": MARKET_DATA_SCHEMA_VERSION,
                "dataset_id": self.dataset_id,
                "version": self.version,
                "name": self.name,
                "asset_class": self.asset_class.value,
                "freq": self.freq.value,
                "adjustment": self.adjustment.value,
                "calendar_id": self.calendar_id,
                "timezone": self.timezone,
                "bar_timestamp_convention": self.bar_timestamp_convention.value,
            }
        )


@dataclass(frozen=True)
class MarketDataSourceSpec:
    source_id: str
    dataset_id: str
    dataset_version: str
    source_type: SourceType
    path: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    field_mapping: Mapping[str, str]
    symbol_mapping: Mapping[str, str] = field(default_factory=dict)
    strict_mode: bool = True

    def __post_init__(self) -> None:
        for field_name in ("source_id", "dataset_id", "dataset_version", "path"):
            _required_text(getattr(self, field_name), field_name)
        required = {"symbol", "exchange", "open", "high", "low", "close", "volume"}
        missing = sorted(field for field in required if not self.field_mapping.get(field))
        if missing:
            raise ValueError(f"field_mapping is missing required fields: {', '.join(missing)}")
        if self.source_type not in {SourceType.CSV, SourceType.PARQUET}:
            raise ValueError("immutable market-data sources must be CSV or PARQUET")

    @property
    def spec_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": MARKET_DATA_SCHEMA_VERSION,
                "source_id": self.source_id,
                "dataset_id": self.dataset_id,
                "dataset_version": self.dataset_version,
                "source_type": self.source_type.value,
                "trading_date": self.trading_date.isoformat(),
                "known_at": _canonical_datetime(self.known_at),
                "source_data_cutoff": _canonical_datetime(self.source_data_cutoff),
                "field_mapping": dict(sorted(self.field_mapping.items())),
                "symbol_mapping": dict(sorted(self.symbol_mapping.items())),
                "strict_mode": self.strict_mode,
            }
        )

    def to_source_spec(self, definition: MarketDatasetDefinition) -> SourceSpec:
        if self.dataset_id != definition.dataset_id or self.dataset_version != definition.version:
            raise ValueError("market-data source does not match dataset definition")
        timestamp_field = "date" if definition.freq == Frequency.D1 else "datetime"
        if timestamp_field == "datetime" and not (
            self.field_mapping.get("datetime") or self.field_mapping.get("bar_start_time")
        ):
            raise ValueError("minute field_mapping requires datetime or bar_start_time")
        if timestamp_field == "date" and not self.field_mapping.get("date"):
            raise ValueError("daily field_mapping requires date")
        return SourceSpec(
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            source_type=self.source_type,
            path=self.path,
            freq=definition.freq,
            timezone=definition.timezone,
            adjustment=definition.adjustment,
            field_mapping=self.field_mapping,
            symbol_mapping=self.symbol_mapping,
            calendar_id=definition.calendar_id,
            strict_mode=self.strict_mode,
            bar_timestamp_convention=definition.bar_timestamp_convention,
        )


def canonical_bar_payload(bar: BarRecord) -> dict[str, object]:
    return {
        "dataset_id": bar.dataset_id,
        "symbol": bar.symbol,
        "exchange": bar.exchange,
        "asset_class": bar.asset_class.value,
        "freq": bar.freq.value,
        "trading_date": bar.trading_date.isoformat(),
        "bar_start_time": _canonical_datetime(bar.bar_start_time),
        "bar_end_time": _canonical_datetime(bar.bar_end_time),
        "open": _canonical_decimal(bar.open),
        "high": _canonical_decimal(bar.high),
        "low": _canonical_decimal(bar.low),
        "close": _canonical_decimal(bar.close),
        "volume": _canonical_decimal(bar.volume),
        "turnover": _canonical_decimal(bar.turnover),
        "adjustment": bar.adjustment.value,
    }


@dataclass(frozen=True)
class MarketDataPartition:
    partition_id: str
    dataset_id: str
    dataset_version: str
    trading_date: date
    known_at: datetime
    source_data_cutoff: datetime
    definition_hash: str
    content_hash: str
    source_ref: str
    source_file_hash: str
    bars: tuple[BarRecord, ...]

    @classmethod
    def create(
        cls,
        definition: MarketDatasetDefinition,
        spec: MarketDataSourceSpec,
        bars: tuple[BarRecord, ...],
        *,
        source_file_hash: str,
    ) -> MarketDataPartition:
        ordered = tuple(
            sorted(
                bars,
                key=lambda bar: (
                    bar.symbol,
                    bar.bar_start_time,
                    bar.exchange,
                    bar.source_row_id or "",
                ),
            )
        )
        content_hash = canonical_hash(
            {
                "schema_version": MARKET_DATA_SCHEMA_VERSION,
                "definition_hash": definition.definition_hash,
                "trading_date": spec.trading_date.isoformat(),
                "known_at": _canonical_datetime(spec.known_at),
                "source_data_cutoff": _canonical_datetime(spec.source_data_cutoff),
                "bars": [canonical_bar_payload(bar) for bar in ordered],
            }
        )
        return cls(
            partition_id=f"market-partition-{content_hash.removeprefix('sha256:')[:24]}",
            dataset_id=definition.dataset_id,
            dataset_version=definition.version,
            trading_date=spec.trading_date,
            known_at=spec.known_at,
            source_data_cutoff=spec.source_data_cutoff,
            definition_hash=definition.definition_hash,
            content_hash=content_hash,
            source_ref=str(Path(spec.path)),
            source_file_hash=source_file_hash,
            bars=ordered,
        )


@dataclass(frozen=True)
class MarketDataImportRun:
    import_run_id: str
    source_id: str
    dataset_id: str
    dataset_version: str
    trading_date: date
    source_file_hash: str
    import_fingerprint: str
    status: ImportStatus
    started_at: datetime
    finished_at: datetime | None = None
    partition_id: str | None = None
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
        spec: MarketDataSourceSpec,
        source_file_hash: str,
        definition_hash: str,
    ) -> MarketDataImportRun:
        fingerprint = canonical_hash(
            {
                "schema_version": MARKET_DATA_SCHEMA_VERSION,
                "source_file_hash": source_file_hash,
                "source_spec_hash": spec.spec_hash,
                "definition_hash": definition_hash,
            }
        )
        return cls(
            import_run_id=import_run_id,
            source_id=spec.source_id,
            dataset_id=spec.dataset_id,
            dataset_version=spec.dataset_version,
            trading_date=spec.trading_date,
            source_file_hash=source_file_hash,
            import_fingerprint=fingerprint,
            status=ImportStatus.CREATED,
            started_at=datetime.now(UTC),
        )


@dataclass(frozen=True)
class MarketDataSnapshotSetItem:
    trading_date: date
    partition_id: str
    content_hash: str


@dataclass(frozen=True)
class MarketDataSnapshotSet:
    snapshot_set_id: str
    dataset_id: str
    dataset_version: str
    definition_hash: str
    date_start: date
    date_end: date
    snapshot_set_hash: str
    items: tuple[MarketDataSnapshotSetItem, ...]
    created_at: datetime

    @property
    def ref(self) -> MarketDataRef:
        return MarketDataRef(self.snapshot_set_id)

    @classmethod
    def create(
        cls,
        *,
        dataset_id: str,
        dataset_version: str,
        definition_hash: str,
        items: tuple[MarketDataSnapshotSetItem, ...],
        created_at: datetime | None = None,
    ) -> MarketDataSnapshotSet:
        if not items:
            raise ValueError("snapshot set items must not be empty")
        ordered = tuple(sorted(items, key=lambda item: item.trading_date))
        dates = [item.trading_date for item in ordered]
        if len(dates) != len(set(dates)):
            raise ValueError("snapshot set contains duplicate trading dates")
        snapshot_set_hash = canonical_hash(
            {
                "schema_version": MARKET_DATA_SCHEMA_VERSION,
                "definition_hash": definition_hash,
                "items": [
                    {
                        "trading_date": item.trading_date.isoformat(),
                        "partition_id": item.partition_id,
                        "content_hash": item.content_hash,
                    }
                    for item in ordered
                ],
            }
        )
        return cls(
            snapshot_set_id=f"market-data-set-{snapshot_set_hash.removeprefix('sha256:')[:24]}",
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            definition_hash=definition_hash,
            date_start=ordered[0].trading_date,
            date_end=ordered[-1].trading_date,
            snapshot_set_hash=snapshot_set_hash,
            items=ordered,
            created_at=created_at or datetime.now(UTC),
        )


@dataclass(frozen=True)
class MarketDataRef:
    snapshot_set_id: str

    def __post_init__(self) -> None:
        _required_text(self.snapshot_set_id, "snapshot_set_id")

    @property
    def uri(self) -> str:
        return f"duckdb://curated_market_bar?{urlencode({'snapshot_set_id': self.snapshot_set_id})}"

    @classmethod
    def parse(cls, value: MarketDataRef | str) -> MarketDataRef:
        if isinstance(value, cls):
            return value
        parsed = urlparse(value)
        table = parsed.netloc or parsed.path.lstrip("/")
        filters = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if parsed.scheme != "duckdb" or table != "curated_market_bar":
            raise ValueError("market-data ref must point to duckdb://curated_market_bar")
        if set(filters) != {"snapshot_set_id"}:
            raise ValueError("market-data ref requires only snapshot_set_id")
        return cls(snapshot_set_id=filters["snapshot_set_id"])

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class ResolvedMarketData:
    market_data_ref: MarketDataRef
    dataset_id: str
    dataset_version: str
    asset_class: AssetClass
    freq: Frequency
    adjustment: Adjustment
    calendar_id: str
    timezone: str
    definition_hash: str
    snapshot_set_hash: str
    items: tuple[MarketDataSnapshotSetItem, ...]

    @property
    def trading_dates(self) -> tuple[date, ...]:
        return tuple(item.trading_date for item in self.items)

    @property
    def partition_ids(self) -> tuple[str, ...]:
        return tuple(item.partition_id for item in self.items)
