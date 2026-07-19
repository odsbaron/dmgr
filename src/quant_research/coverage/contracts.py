from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum

from quant_research.contracts.bar import Frequency
from quant_research.contracts.refs import DataRef
from quant_research.temporal_assets import canonical_hash, required_text


COVERAGE_SCHEMA_VERSION = "1"


class CoveragePolicy(StrEnum):
    STRICT = "STRICT"
    WARNING = "WARNING"


class CoverageRunStatus(StrEnum):
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class CoverageScope(StrEnum):
    RUN = "RUN"
    DATE = "DATE"
    SYMBOL_DATE = "SYMBOL_DATE"


class CoverageIssueSeverity(StrEnum):
    WARNING = "WARNING"
    ERROR = "ERROR"


class TimestampConvention(StrEnum):
    BAR_START = "BAR_START"
    BAR_END = "BAR_END"


@dataclass(frozen=True)
class CoverageRunRequest:
    coverage_run_id: str
    market_data_ref: str
    calendar_ref: str
    universe_ref: str
    daily_status_ref: str
    date_start: date
    date_end: date
    freq: Frequency
    timestamp_convention: TimestampConvention
    policy: CoveragePolicy = CoveragePolicy.STRICT
    minimum_coverage_ratio: float = 1.0

    def __post_init__(self) -> None:
        for field_name in (
            "coverage_run_id",
            "market_data_ref",
            "calendar_ref",
            "universe_ref",
            "daily_status_ref",
        ):
            required_text(getattr(self, field_name), field_name)
        if self.date_start > self.date_end:
            raise ValueError("date_start must be <= date_end")
        if not 0.0 <= self.minimum_coverage_ratio <= 1.0:
            raise ValueError("minimum_coverage_ratio must be between 0 and 1")

    @property
    def config_hash(self) -> str:
        return canonical_hash(
            {
                "schema_version": COVERAGE_SCHEMA_VERSION,
                "coverage_run_id": self.coverage_run_id,
                "market_data_ref": self.market_data_ref,
                "calendar_ref": self.calendar_ref,
                "universe_ref": self.universe_ref,
                "daily_status_ref": self.daily_status_ref,
                "date_start": self.date_start.isoformat(),
                "date_end": self.date_end.isoformat(),
                "freq": self.freq.value,
                "timestamp_convention": self.timestamp_convention.value,
                "policy": self.policy.value,
                "minimum_coverage_ratio": self.minimum_coverage_ratio,
            }
        )


@dataclass(frozen=True, order=True)
class ExpectedSlot:
    trading_date: date
    symbol: str
    expected_at: datetime | None

    def comparison_key(self, freq: Frequency) -> tuple[str, date, datetime | None]:
        return self.symbol, self.trading_date, None if freq == Frequency.D1 else self.expected_at


@dataclass(frozen=True)
class CoverageMetric:
    coverage_run_id: str
    scope: CoverageScope
    expected_bar_count: int
    actual_bar_count: int
    matched_bar_count: int
    missing_bar_count: int
    unexpected_bar_count: int
    coverage_ratio: float
    trading_date: date | None = None
    symbol: str | None = None

    def __post_init__(self) -> None:
        required_text(self.coverage_run_id, "coverage_run_id")
        counts = (
            self.expected_bar_count,
            self.actual_bar_count,
            self.matched_bar_count,
            self.missing_bar_count,
            self.unexpected_bar_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("coverage metric counts must be non-negative")
        if not 0.0 <= self.coverage_ratio <= 1.0:
            raise ValueError("coverage_ratio must be between 0 and 1")
        if self.scope == CoverageScope.RUN and (self.trading_date or self.symbol):
            raise ValueError("RUN metric must not define trading_date or symbol")
        if self.scope == CoverageScope.DATE and (self.trading_date is None or self.symbol):
            raise ValueError("DATE metric requires only trading_date")
        if self.scope == CoverageScope.SYMBOL_DATE and (
            self.trading_date is None or not self.symbol
        ):
            raise ValueError("SYMBOL_DATE metric requires trading_date and symbol")


@dataclass(frozen=True)
class CoverageIssue:
    coverage_run_id: str
    issue_code: str
    severity: CoverageIssueSeverity
    message: str
    trading_date: date | None = None
    symbol: str | None = None
    expected_at: datetime | None = None
    actual_at: datetime | None = None

    def __post_init__(self) -> None:
        required_text(self.coverage_run_id, "coverage_run_id")
        required_text(self.issue_code, "issue_code")
        required_text(self.message, "message")

    @property
    def issue_id(self) -> str:
        digest = canonical_hash(
            {
                "coverage_run_id": self.coverage_run_id,
                "issue_code": self.issue_code,
                "severity": self.severity.value,
                "message": self.message,
                "trading_date": self.trading_date.isoformat() if self.trading_date else None,
                "symbol": self.symbol,
                "expected_at": self.expected_at.isoformat() if self.expected_at else None,
                "actual_at": self.actual_at.isoformat() if self.actual_at else None,
            }
        )
        return f"coverage-issue-{digest.removeprefix('sha256:')[:24]}"


@dataclass(frozen=True)
class CoverageAnalysis:
    metrics: tuple[CoverageMetric, ...]
    issues: tuple[CoverageIssue, ...]
    consumable: bool

    @property
    def run_metric(self) -> CoverageMetric:
        return next(metric for metric in self.metrics if metric.scope == CoverageScope.RUN)


@dataclass(frozen=True)
class CoverageRunManifest:
    coverage_run_id: str
    config_hash: str
    status: CoverageRunStatus
    policy: CoveragePolicy
    timestamp_convention: TimestampConvention
    freq: Frequency
    date_start: date
    date_end: date
    minimum_coverage_ratio: float
    market_data_ref: str
    calendar_ref: str
    universe_ref: str
    daily_status_ref: str
    market_data_hash: str | None
    calendar_hash: str | None
    universe_hash: str | None
    daily_status_hash: str | None
    expected_bar_count: int
    actual_bar_count: int
    matched_bar_count: int
    missing_bar_count: int
    unexpected_bar_count: int
    coverage_ratio: float
    issue_count: int
    consumable: bool
    started_at: datetime
    finished_at: datetime
    code_version: str
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_analysis(
        cls,
        request: CoverageRunRequest,
        analysis: CoverageAnalysis,
        *,
        input_hashes: dict[str, str],
        started_at: datetime,
        code_version: str,
    ) -> CoverageRunManifest:
        metric = analysis.run_metric
        return cls(
            coverage_run_id=request.coverage_run_id,
            config_hash=request.config_hash,
            status=CoverageRunStatus.COMMITTED,
            policy=request.policy,
            timestamp_convention=request.timestamp_convention,
            freq=request.freq,
            date_start=request.date_start,
            date_end=request.date_end,
            minimum_coverage_ratio=request.minimum_coverage_ratio,
            market_data_ref=request.market_data_ref,
            calendar_ref=request.calendar_ref,
            universe_ref=request.universe_ref,
            daily_status_ref=request.daily_status_ref,
            market_data_hash=input_hashes["market_data"],
            calendar_hash=input_hashes["calendar"],
            universe_hash=input_hashes["universe"],
            daily_status_hash=input_hashes["daily_status"],
            expected_bar_count=metric.expected_bar_count,
            actual_bar_count=metric.actual_bar_count,
            matched_bar_count=metric.matched_bar_count,
            missing_bar_count=metric.missing_bar_count,
            unexpected_bar_count=metric.unexpected_bar_count,
            coverage_ratio=metric.coverage_ratio,
            issue_count=len(analysis.issues),
            consumable=analysis.consumable,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            code_version=code_version,
        )

    @classmethod
    def failed(
        cls,
        request: CoverageRunRequest,
        *,
        started_at: datetime,
        code_version: str,
        error_code: str,
        error_message: str,
    ) -> CoverageRunManifest:
        return cls(
            coverage_run_id=request.coverage_run_id,
            config_hash=request.config_hash,
            status=CoverageRunStatus.FAILED,
            policy=request.policy,
            timestamp_convention=request.timestamp_convention,
            freq=request.freq,
            date_start=request.date_start,
            date_end=request.date_end,
            minimum_coverage_ratio=request.minimum_coverage_ratio,
            market_data_ref=request.market_data_ref,
            calendar_ref=request.calendar_ref,
            universe_ref=request.universe_ref,
            daily_status_ref=request.daily_status_ref,
            market_data_hash=None,
            calendar_hash=None,
            universe_hash=None,
            daily_status_hash=None,
            expected_bar_count=0,
            actual_bar_count=0,
            matched_bar_count=0,
            missing_bar_count=0,
            unexpected_bar_count=0,
            coverage_ratio=0.0,
            issue_count=1,
            consumable=False,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            code_version=code_version,
            error_code=error_code,
            error_message=error_message,
        )


@dataclass(frozen=True)
class CoverageReportRef:
    coverage_run_id: str

    def __post_init__(self) -> None:
        required_text(self.coverage_run_id, "coverage_run_id")

    @property
    def uri(self) -> str:
        return DataRef(
            "coverage_run_manifest",
            {"coverage_run_id": self.coverage_run_id},
        ).uri

    @classmethod
    def parse(cls, value: CoverageReportRef | DataRef | str) -> CoverageReportRef:
        if isinstance(value, cls):
            return value
        ref = DataRef.parse(value) if isinstance(value, str) else value
        if ref.table != "coverage_run_manifest":
            raise ValueError("coverage report ref must point to coverage_run_manifest")
        if set(ref.filters) != {"coverage_run_id"} or not ref.filters["coverage_run_id"]:
            raise ValueError("coverage report ref requires only coverage_run_id")
        return cls(ref.filters["coverage_run_id"])

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class CoverageRunResult:
    coverage_run_id: str
    status: CoverageRunStatus
    consumable: bool
    expected_bar_count: int
    actual_bar_count: int
    matched_bar_count: int
    missing_bar_count: int
    unexpected_bar_count: int
    coverage_ratio: float
    issue_count: int
    manifest_ref: DataRef
    metric_ref: DataRef
    issue_ref: DataRef
    reused_existing: bool = False
    error_code: str | None = None
    error_message: str | None = None


def coverage_metric_ref(coverage_run_id: str) -> DataRef:
    return DataRef("coverage_metric", {"coverage_run_id": coverage_run_id})


def coverage_issue_ref(coverage_run_id: str) -> DataRef:
    return DataRef("coverage_issue", {"coverage_run_id": coverage_run_id})
