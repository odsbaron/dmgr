from __future__ import annotations

from dataclasses import dataclass

from quant_research.market_calendar.contracts import (
    CalendarRef,
    CalendarSnapshotSet,
    CalendarSnapshotSetItem,
    ResolvedMarketCalendar,
)
from quant_research.market_calendar.duckdb_store import CalendarStoreError, LocalDuckDBCalendarStore


@dataclass(frozen=True)
class CalendarResolver:
    store: LocalDuckDBCalendarStore

    def resolve(self, ref: CalendarRef | str) -> ResolvedMarketCalendar:
        calendar_ref = CalendarRef.parse(ref)
        snapshot_set = self.store.read_snapshot_set(calendar_ref)
        definition = self.store.get_definition(
            snapshot_set.calendar_id,
            snapshot_set.calendar_version,
        )
        if definition is None:
            raise CalendarStoreError("UNKNOWN_DEFINITION", "calendar definition does not exist")
        if definition.definition_hash != snapshot_set.definition_hash:
            raise CalendarStoreError("DEFINITION_HASH_MISMATCH", "calendar definition hash mismatch")
        rebuilt = CalendarSnapshotSet.create(
            snapshot_set.calendar_id,
            snapshot_set.calendar_version,
            snapshot_set.definition_hash,
            tuple(
                CalendarSnapshotSetItem(item.calendar_date, item.snapshot_id, item.content_hash)
                for item in snapshot_set.items
            ),
        )
        if rebuilt.snapshot_set_hash != snapshot_set.snapshot_set_hash:
            raise CalendarStoreError("SNAPSHOT_SET_HASH_MISMATCH", "calendar set hash mismatch")
        days = {}
        for item in snapshot_set.items:
            snapshot = self.store.get_snapshot(item.snapshot_id)
            if snapshot is None:
                raise CalendarStoreError("UNKNOWN_SNAPSHOT", f"unknown snapshot: {item.snapshot_id}")
            if (
                snapshot.calendar_id != snapshot_set.calendar_id
                or snapshot.calendar_version != snapshot_set.calendar_version
                or snapshot.calendar_date != item.calendar_date
                or snapshot.definition_hash != snapshot_set.definition_hash
                or snapshot.content_hash != item.content_hash
            ):
                raise CalendarStoreError("SNAPSHOT_IDENTITY_MISMATCH", "calendar snapshot mismatch")
            days[item.calendar_date] = snapshot
        return ResolvedMarketCalendar(
            calendar_ref=calendar_ref,
            calendar_id=definition.calendar_id,
            calendar_version=definition.version,
            timezone=definition.timezone,
            definition_hash=definition.definition_hash,
            snapshot_set_hash=snapshot_set.snapshot_set_hash,
            days_by_date=days,
        )
