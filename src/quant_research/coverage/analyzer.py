from __future__ import annotations

from datetime import date, datetime

from quant_research.contracts.bar import BarRecord, Frequency
from quant_research.coverage.contracts import (
    CoverageAnalysis,
    CoverageIssue,
    CoverageIssueSeverity,
    CoverageMetric,
    CoveragePolicy,
    CoverageRunRequest,
    CoverageScope,
    ExpectedSlot,
    TimestampConvention,
)
from quant_research.coverage.expected_slots import ExpectedSlotGeneration


SlotKey = tuple[str, date, datetime | None]


class CoverageAnalyzer:
    def analyze(
        self,
        request: CoverageRunRequest,
        generation: ExpectedSlotGeneration,
        bars: list[BarRecord],
    ) -> CoverageAnalysis:
        expected_by_key = {slot.comparison_key(request.freq): slot for slot in generation.slots}
        actual_by_key: dict[SlotKey, BarRecord] = {}
        duplicate_issues: list[CoverageIssue] = []

        for bar in bars:
            symbol_date = (bar.symbol, bar.trading_date)
            if (
                bar.freq != request.freq
                or not request.date_start <= bar.trading_date <= request.date_end
                or symbol_date not in generation.scoped_symbol_dates
            ):
                continue
            key = self._bar_key(bar, request)
            if key in actual_by_key:
                duplicate_issues.append(
                    CoverageIssue(
                        coverage_run_id=request.coverage_run_id,
                        issue_code="DUPLICATE_ACTUAL_SLOT",
                        severity=CoverageIssueSeverity.ERROR,
                        message="Multiple actual bars share one coverage comparison key",
                        trading_date=bar.trading_date,
                        symbol=bar.symbol,
                        actual_at=key[2],
                    )
                )
            else:
                actual_by_key[key] = bar

        expected_keys = set(expected_by_key)
        comparable_actual_keys = {
            key for key in actual_by_key if (key[0], key[1]) in generation.resolved_symbol_dates
        }
        missing_keys = expected_keys - comparable_actual_keys
        unexpected_keys = comparable_actual_keys - expected_keys

        issues = [*generation.issues, *duplicate_issues]
        issues.extend(self._missing_issue(request, expected_by_key[key]) for key in missing_keys)
        issues.extend(
            self._unexpected_issue(request, actual_by_key[key], key) for key in unexpected_keys
        )

        metrics = self._metrics(
            request,
            generation,
            expected_keys,
            actual_by_key,
            missing_keys,
            unexpected_keys,
        )
        ordered_issues = tuple(sorted(issues, key=lambda issue: issue.issue_id))
        run_metric = next(metric for metric in metrics if metric.scope == CoverageScope.RUN)
        has_error = any(issue.severity == CoverageIssueSeverity.ERROR for issue in ordered_issues)
        if request.policy == CoveragePolicy.STRICT:
            consumable = (
                not has_error
                and run_metric.coverage_ratio >= request.minimum_coverage_ratio
                and run_metric.missing_bar_count == 0
                and run_metric.unexpected_bar_count == 0
            )
        else:
            consumable = not has_error
        return CoverageAnalysis(metrics=metrics, issues=ordered_issues, consumable=consumable)

    def _metrics(
        self,
        request: CoverageRunRequest,
        generation: ExpectedSlotGeneration,
        expected_keys: set[SlotKey],
        actual_by_key: dict[SlotKey, BarRecord],
        missing_keys: set[SlotKey],
        unexpected_keys: set[SlotKey],
    ) -> tuple[CoverageMetric, ...]:
        groups = sorted(generation.scoped_symbol_dates, key=lambda item: (item[1], item[0]))
        metrics: list[CoverageMetric] = []
        for symbol, trading_date in groups:
            group = (symbol, trading_date)
            expected = {key for key in expected_keys if key[:2] == group}
            actual = {key for key in actual_by_key if key[:2] == group}
            missing = {key for key in missing_keys if key[:2] == group}
            unexpected = {key for key in unexpected_keys if key[:2] == group}
            metrics.append(
                self._metric(
                    request.coverage_run_id,
                    CoverageScope.SYMBOL_DATE,
                    expected,
                    actual,
                    missing,
                    unexpected,
                    trading_date=trading_date,
                    symbol=symbol,
                )
            )

        for trading_date in sorted({value[1] for value in groups}):
            expected = {key for key in expected_keys if key[1] == trading_date}
            actual = {key for key in actual_by_key if key[1] == trading_date}
            missing = {key for key in missing_keys if key[1] == trading_date}
            unexpected = {key for key in unexpected_keys if key[1] == trading_date}
            metrics.append(
                self._metric(
                    request.coverage_run_id,
                    CoverageScope.DATE,
                    expected,
                    actual,
                    missing,
                    unexpected,
                    trading_date=trading_date,
                )
            )

        metrics.append(
            self._metric(
                request.coverage_run_id,
                CoverageScope.RUN,
                expected_keys,
                set(actual_by_key),
                missing_keys,
                unexpected_keys,
            )
        )
        return tuple(metrics)

    @staticmethod
    def _metric(
        coverage_run_id: str,
        scope: CoverageScope,
        expected: set[SlotKey],
        actual: set[SlotKey],
        missing: set[SlotKey],
        unexpected: set[SlotKey],
        *,
        trading_date: date | None = None,
        symbol: str | None = None,
    ) -> CoverageMetric:
        matched = len(expected) - len(missing)
        if expected:
            ratio = matched / len(expected)
        else:
            ratio = 1.0 if not actual else 0.0
        return CoverageMetric(
            coverage_run_id=coverage_run_id,
            scope=scope,
            trading_date=trading_date,
            symbol=symbol,
            expected_bar_count=len(expected),
            actual_bar_count=len(actual),
            matched_bar_count=matched,
            missing_bar_count=len(missing),
            unexpected_bar_count=len(unexpected),
            coverage_ratio=ratio,
        )

    @staticmethod
    def _bar_key(bar: BarRecord, request: CoverageRunRequest) -> SlotKey:
        if request.freq == Frequency.D1:
            as_of = None
        elif request.timestamp_convention == TimestampConvention.BAR_START:
            as_of = bar.bar_start_time
        else:
            as_of = bar.bar_end_time
        return bar.symbol, bar.trading_date, as_of

    @staticmethod
    def _missing_issue(request: CoverageRunRequest, slot: ExpectedSlot) -> CoverageIssue:
        return CoverageIssue(
            coverage_run_id=request.coverage_run_id,
            issue_code="MISSING_EXPECTED_SLOT",
            severity=CoverageIssueSeverity.WARNING,
            message="Expected bar slot has no matching actual bar",
            trading_date=slot.trading_date,
            symbol=slot.symbol,
            expected_at=slot.expected_at,
        )

    @staticmethod
    def _unexpected_issue(
        request: CoverageRunRequest,
        bar: BarRecord,
        key: SlotKey,
    ) -> CoverageIssue:
        return CoverageIssue(
            coverage_run_id=request.coverage_run_id,
            issue_code="UNEXPECTED_ACTUAL_SLOT",
            severity=CoverageIssueSeverity.WARNING,
            message="Actual bar has no matching expected slot",
            trading_date=bar.trading_date,
            symbol=bar.symbol,
            actual_at=key[2],
        )
