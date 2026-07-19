from __future__ import annotations

from dataclasses import dataclass

from quant_research.daily_status.contracts import (
    DailyStatusRef,
    ResolvedDailyStatus,
    StatusSnapshotSet,
    StatusSnapshotSetItem,
)
from quant_research.daily_status.duckdb_store import (
    DailyStatusStoreError,
    LocalDuckDBDailyStatusStore,
)


@dataclass(frozen=True)
class DailyStatusResolver:
    store: LocalDuckDBDailyStatusStore

    def resolve(self, ref: DailyStatusRef | str) -> ResolvedDailyStatus:
        status_ref = DailyStatusRef.parse(ref)
        snapshot_set = self.store.read_snapshot_set(status_ref)
        definition = self.store.get_definition(
            snapshot_set.status_id,
            snapshot_set.status_version,
        )
        if definition is None:
            raise DailyStatusStoreError("UNKNOWN_DEFINITION", "status definition does not exist")
        if definition.definition_hash != snapshot_set.definition_hash:
            raise DailyStatusStoreError("DEFINITION_HASH_MISMATCH", "status definition hash mismatch")
        rebuilt = StatusSnapshotSet.create(
            snapshot_set.status_id,
            snapshot_set.status_version,
            snapshot_set.definition_hash,
            tuple(
                StatusSnapshotSetItem(item.trading_date, item.snapshot_id, item.content_hash)
                for item in snapshot_set.items
            ),
        )
        if rebuilt.snapshot_set_hash != snapshot_set.snapshot_set_hash:
            raise DailyStatusStoreError("SNAPSHOT_SET_HASH_MISMATCH", "status set hash mismatch")
        statuses_by_date = {}
        for item in snapshot_set.items:
            snapshot = self.store.get_snapshot(item.snapshot_id)
            if snapshot is None:
                raise DailyStatusStoreError("UNKNOWN_SNAPSHOT", f"unknown snapshot: {item.snapshot_id}")
            if (
                snapshot.status_id != snapshot_set.status_id
                or snapshot.status_version != snapshot_set.status_version
                or snapshot.trading_date != item.trading_date
                or snapshot.definition_hash != snapshot_set.definition_hash
                or snapshot.content_hash != item.content_hash
            ):
                raise DailyStatusStoreError("SNAPSHOT_IDENTITY_MISMATCH", "status snapshot mismatch")
            statuses_by_date[item.trading_date] = {
                status.instrument_id: status for status in snapshot.statuses
            }
        return ResolvedDailyStatus(
            status_ref=status_ref,
            status_id=definition.status_id,
            status_version=definition.version,
            asset_class=definition.asset_class,
            calendar_id=definition.calendar_id,
            calendar_version=definition.calendar_version,
            timezone=definition.timezone,
            definition_hash=definition.definition_hash,
            snapshot_set_hash=snapshot_set.snapshot_set_hash,
            statuses_by_date=statuses_by_date,
        )
