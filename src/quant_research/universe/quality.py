from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from quant_research.universe.contracts import (
    UniverseDefinition,
    UniverseMember,
    UniverseSourceSpec,
    canonical_hash,
)


class UniverseQualitySeverity(StrEnum):
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class UniverseQualityIssue:
    issue_id: str
    import_run_id: str
    universe_id: str
    universe_version: str
    trading_date: str
    issue_code: str
    severity: UniverseQualitySeverity
    message: str
    instrument_id: str | None = None
    source_row_id: str | None = None
    raw_ref: str | None = None

    @property
    def is_blocking(self) -> bool:
        return self.severity == UniverseQualitySeverity.ERROR


@dataclass(frozen=True)
class UniverseQualityReport:
    import_run_id: str
    issues: tuple[UniverseQualityIssue, ...]

    @property
    def issue_count(self) -> int:
        return len(self.issues)

    @property
    def has_blocking_errors(self) -> bool:
        return any(issue.is_blocking for issue in self.issues)


@dataclass(frozen=True)
class UniverseQualityValidator:
    import_run_id: str

    def validate(
        self,
        definition: UniverseDefinition,
        spec: UniverseSourceSpec,
        members: tuple[UniverseMember, ...],
    ) -> UniverseQualityReport:
        issues: list[UniverseQualityIssue] = []
        if definition.key != (spec.universe_id, spec.universe_version):
            issues.append(
                self._issue(
                    spec,
                    "DEFINITION_SOURCE_MISMATCH",
                    "source Universe id/version does not match the registered definition",
                )
            )
        if not members:
            issues.append(self._issue(spec, "EMPTY_SNAPSHOT", "snapshot has no members"))

        issues.extend(self._metadata_issues(definition, spec))
        seen: set[str] = set()
        for member in members:
            if not member.instrument_id:
                issues.append(
                    self._member_issue(spec, member, "EMPTY_INSTRUMENT_ID", "instrument_id is empty")
                )
            elif member.instrument_id in seen:
                issues.append(
                    self._member_issue(
                        spec,
                        member,
                        "DUPLICATE_MEMBER",
                        f"duplicate instrument_id: {member.instrument_id}",
                    )
                )
            seen.add(member.instrument_id)
            if (
                member.declared_trading_date is not None
                and member.declared_trading_date != spec.trading_date
            ):
                issues.append(
                    self._member_issue(
                        spec,
                        member,
                        "PARTITION_DATE_MISMATCH",
                        "member trading_date does not match source partition",
                    )
                )
            if member.weight is not None and member.weight < 0:
                issues.append(
                    self._member_issue(
                        spec,
                        member,
                        "INVALID_WEIGHT",
                        "member weight must be non-negative",
                    )
                )
            if member.rank is not None and member.rank < 1:
                issues.append(
                    self._member_issue(
                        spec,
                        member,
                        "INVALID_RANK",
                        "member rank must be >= 1",
                    )
                )
        return UniverseQualityReport(self.import_run_id, tuple(issues))

    def _metadata_issues(
        self,
        definition: UniverseDefinition,
        spec: UniverseSourceSpec,
    ) -> list[UniverseQualityIssue]:
        issues: list[UniverseQualityIssue] = []
        if not _is_aware(spec.known_at) or not _is_aware(spec.source_data_cutoff):
            issues.append(
                self._issue(
                    spec,
                    "NAIVE_POINT_IN_TIME",
                    "known_at and source_data_cutoff must be timezone-aware",
                )
            )
            return issues
        if spec.source_data_cutoff > spec.known_at:
            issues.append(
                self._issue(
                    spec,
                    "FUTURE_SOURCE_CUTOFF",
                    "source_data_cutoff must be <= known_at",
                )
            )
        if spec.known_at > definition.selection_cutoff(spec.trading_date):
            issues.append(
                self._issue(
                    spec,
                    "LATE_KNOWN_AT",
                    "known_at is after the configured daily selection cutoff",
                )
            )
        return issues

    def _member_issue(
        self,
        spec: UniverseSourceSpec,
        member: UniverseMember,
        code: str,
        message: str,
    ) -> UniverseQualityIssue:
        return self._issue(
            spec,
            code,
            message,
            instrument_id=member.instrument_id or None,
            source_row_id=member.source_row_id,
            raw_ref=member.raw_ref,
        )

    def _issue(
        self,
        spec: UniverseSourceSpec,
        code: str,
        message: str,
        *,
        instrument_id: str | None = None,
        source_row_id: str | None = None,
        raw_ref: str | None = None,
    ) -> UniverseQualityIssue:
        issue_id = canonical_hash(
            {
                "import_run_id": self.import_run_id,
                "issue_code": code,
                "instrument_id": instrument_id,
                "source_row_id": source_row_id,
            }
        )
        return UniverseQualityIssue(
            issue_id=issue_id,
            import_run_id=self.import_run_id,
            universe_id=spec.universe_id,
            universe_version=spec.universe_version,
            trading_date=spec.trading_date.isoformat(),
            issue_code=code,
            severity=UniverseQualitySeverity.ERROR,
            message=message,
            instrument_id=instrument_id,
            source_row_id=source_row_id,
            raw_ref=raw_ref,
        )


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
