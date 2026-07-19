from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from quant_research.contracts.bar import BarRecord
from quant_research.contracts.quality import QualityIssue, QualityReport, Severity
from quant_research.data.partition_contracts import (
    MarketDataSourceSpec,
    MarketDatasetDefinition,
)
from quant_research.data.quality import KLineQualityValidator


@dataclass(frozen=True)
class MarketDataPartitionQualityValidator:
    import_run_id: str
    definition: MarketDatasetDefinition
    spec: MarketDataSourceSpec

    def validate(self, bars: Iterable[BarRecord]) -> QualityReport:
        bar_list = list(bars)
        issues = list(
            KLineQualityValidator(
                self.import_run_id,
                calendar_id=self.definition.calendar_id,
                timezone=self.definition.timezone,
            )
            .validate(bar_list)
            .issues
        )

        if not bar_list:
            issues.append(self._issue("EMPTY_PARTITION", "market-data partition is empty"))
        if (
            self.spec.dataset_id != self.definition.dataset_id
            or self.spec.dataset_version != self.definition.version
        ):
            issues.append(
                self._issue(
                    "DEFINITION_SOURCE_MISMATCH",
                    "source dataset id/version does not match definition",
                )
            )

        known_at_aware = self._is_aware(self.spec.known_at)
        cutoff_aware = self._is_aware(self.spec.source_data_cutoff)
        if not known_at_aware:
            issues.append(self._issue("NAIVE_KNOWN_AT", "known_at must be timezone-aware"))
        if not cutoff_aware:
            issues.append(
                self._issue(
                    "NAIVE_SOURCE_DATA_CUTOFF",
                    "source_data_cutoff must be timezone-aware",
                )
            )
        if known_at_aware and cutoff_aware and self.spec.source_data_cutoff > self.spec.known_at:
            issues.append(
                self._issue(
                    "SOURCE_CUTOFF_AFTER_KNOWN_AT",
                    "source_data_cutoff must be at or before known_at",
                )
            )

        for bar in bar_list:
            if bar.dataset_id != self.definition.dataset_id:
                issues.append(
                    self._issue(
                        "BAR_DATASET_MISMATCH",
                        "bar dataset id does not match definition",
                        bar,
                    )
                )
            if bar.freq != self.definition.freq:
                issues.append(self._issue("BAR_FREQUENCY_MISMATCH", "bar frequency mismatch", bar))
            if bar.adjustment != self.definition.adjustment:
                issues.append(
                    self._issue("BAR_ADJUSTMENT_MISMATCH", "bar adjustment mismatch", bar)
                )
            if bar.asset_class != self.definition.asset_class:
                issues.append(
                    self._issue("BAR_ASSET_CLASS_MISMATCH", "bar asset class mismatch", bar)
                )
            if bar.trading_date != self.spec.trading_date:
                issues.append(
                    self._issue(
                        "PARTITION_DATE_MISMATCH",
                        "bar trading date does not match declared partition date",
                        bar,
                    )
                )
            if not self._is_aware(bar.bar_start_time) or not self._is_aware(bar.bar_end_time):
                issues.append(
                    self._issue("NAIVE_BAR_TIMESTAMP", "bar timestamps must be aware", bar)
                )
            elif cutoff_aware and bar.bar_end_time > self.spec.source_data_cutoff:
                issues.append(
                    self._issue(
                        "BAR_AFTER_SOURCE_CUTOFF",
                        "bar ends after source_data_cutoff",
                        bar,
                    )
                )

        return QualityReport(self.import_run_id, tuple(issues))

    def _issue(
        self,
        code: str,
        message: str,
        bar: BarRecord | None = None,
    ) -> QualityIssue:
        symbol = bar.symbol if bar else None
        bar_start = bar.bar_start_time if bar else None
        source_row_id = bar.source_row_id if bar else None
        identity = (
            f"{symbol or '-'}:{bar_start.isoformat() if bar_start else '-'}:{source_row_id or '-'}"
        )
        return QualityIssue(
            issue_id=f"{self.import_run_id}:{code}:{identity}",
            import_run_id=self.import_run_id,
            dataset_id=self.spec.dataset_id,
            symbol=symbol,
            freq=bar.freq if bar else self.definition.freq,
            trading_date=bar.trading_date if bar else self.spec.trading_date,
            bar_start_time=bar_start,
            issue_code=code,
            severity=Severity.ERROR,
            message=message,
            raw_ref=bar.raw_ref if bar else self.spec.path,
        )

    def _is_aware(self, value) -> bool:
        return value.tzinfo is not None and value.utcoffset() is not None
