from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Iterable
from zoneinfo import ZoneInfo

from quant_research.contracts.bar import BarRecord, Frequency
from quant_research.contracts.quality import QualityIssue, QualityReport, Severity


@dataclass(frozen=True)
class KLineQualityValidator:
    import_run_id: str
    calendar_id: str = "default"
    timezone: str = "UTC"

    def validate(self, bars: Iterable[BarRecord]) -> QualityReport:
        bar_list = list(bars)
        issues: list[QualityIssue] = []
        seen: set[tuple[str, str, str, str, str]] = set()
        previous_start: dict[tuple[str, str, str], object] = {}
        for bar in bar_list:
            key = (
                bar.dataset_id,
                bar.symbol,
                bar.freq.value,
                bar.adjustment.value,
                bar.bar_start_time.isoformat(),
            )
            if key in seen:
                issues.append(self._issue(bar, "DUPLICATE_BAR", "duplicate bar key"))
            seen.add(key)

            if not self._has_valid_ohlc(bar):
                issues.append(self._issue(bar, "INVALID_OHLC", "OHLC values are inconsistent"))

            if self._is_negative(bar.volume):
                issues.append(self._issue(bar, "NEGATIVE_VOLUME", "volume is negative"))

            series_key = (bar.symbol, bar.freq.value, bar.adjustment.value)
            prior_start = previous_start.get(series_key)
            if prior_start is not None and bar.bar_start_time < prior_start:
                issues.append(
                    self._issue(
                        bar,
                        "OUT_OF_ORDER_BAR",
                        "bar_start_time is earlier than the preceding source row",
                    )
                )
            previous_start[series_key] = bar.bar_start_time

        issues.extend(self._window_issues(bar_list))

        return QualityReport(import_run_id=self.import_run_id, issues=tuple(issues))

    def _window_issues(self, bars: list[BarRecord]) -> list[QualityIssue]:
        grouped: defaultdict[tuple[str, str, str], list[BarRecord]] = defaultdict(list)
        for bar in bars:
            if bar.freq != Frequency.D1:
                grouped[(bar.symbol, bar.freq.value, bar.trading_date.isoformat())].append(bar)

        issues: list[QualityIssue] = []
        for group in grouped.values():
            ordered = sorted(group, key=lambda item: item.bar_start_time)
            for previous, current in zip(ordered, ordered[1:], strict=False):
                if current.bar_start_time < previous.bar_end_time:
                    issues.append(
                        self._issue(
                            current,
                            "OVERLAPPING_BAR_WINDOW",
                            "bar window overlaps the preceding bar",
                        )
                    )
                    continue
                if current.bar_start_time == previous.bar_end_time:
                    continue
                if self._is_configured_session_break(previous, current):
                    continue
                interval = self._frequency_delta(current.freq)
                gap = current.bar_start_time - previous.bar_end_time
                missing_count = max(1, int(gap / interval))
                issues.append(
                    self._issue(
                        current,
                        "MISSING_BAR_WINDOW",
                        (
                            f"missing {missing_count} expected {current.freq.value} window(s) "
                            f"between {previous.bar_end_time.isoformat()} and "
                            f"{current.bar_start_time.isoformat()}"
                        ),
                    )
                )
        return issues

    def _is_configured_session_break(self, previous: BarRecord, current: BarRecord) -> bool:
        if self.calendar_id.lower() not in {
            "cn_stock_simple",
            "xshg_xshe",
            "xshg",
            "xshe",
        }:
            return False
        zone = ZoneInfo(self.timezone)
        previous_local = previous.bar_end_time.astimezone(zone)
        current_local = current.bar_start_time.astimezone(zone)
        return (
            previous_local.date() == current_local.date()
            and previous_local.time().isoformat() == "11:30:00"
            and current_local.time().isoformat() == "13:00:00"
        )

    def _frequency_delta(self, freq: Frequency) -> timedelta:
        minutes = {
            Frequency.M1: 1,
            Frequency.M5: 5,
            Frequency.M15: 15,
            Frequency.M30: 30,
            Frequency.M60: 60,
        }
        return timedelta(minutes=minutes[freq])

    def _issue(self, bar: BarRecord, code: str, message: str) -> QualityIssue:
        return QualityIssue(
            issue_id=f"{self.import_run_id}:{code}:{bar.symbol}:{bar.bar_start_time.isoformat()}",
            import_run_id=self.import_run_id,
            dataset_id=bar.dataset_id,
            symbol=bar.symbol,
            freq=bar.freq,
            trading_date=bar.trading_date,
            bar_start_time=bar.bar_start_time,
            issue_code=code,
            severity=Severity.ERROR,
            message=message,
            raw_ref=bar.raw_ref,
        )

    def _has_valid_ohlc(self, bar: BarRecord) -> bool:
        try:
            open_price = Decimal(bar.open)
            high = Decimal(bar.high)
            low = Decimal(bar.low)
            close = Decimal(bar.close)
        except InvalidOperation:
            return False
        return high >= max(open_price, close) and low <= min(open_price, close)

    def _is_negative(self, value: str) -> bool:
        try:
            return Decimal(value) < 0
        except InvalidOperation:
            return True
