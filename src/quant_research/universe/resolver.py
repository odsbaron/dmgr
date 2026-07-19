from __future__ import annotations

from dataclasses import dataclass

from quant_research.universe.contracts import (
    DailyUniverseMembership,
    ResolvedUniverse,
    UniverseRef,
)
from quant_research.universe.duckdb_store import LocalDuckDBUniverseStore


@dataclass(frozen=True)
class UniverseResolver:
    store: LocalDuckDBUniverseStore

    def resolve(self, ref: UniverseRef | str) -> ResolvedUniverse:
        universe_ref = UniverseRef.parse(ref)
        snapshot_set = self.store.read_snapshot_set(universe_ref)
        definition = self.store.get_definition(
            snapshot_set.universe_id,
            snapshot_set.universe_version,
        )
        if definition is None:
            raise ValueError("Universe snapshot set references an unknown definition")
        daily: list[DailyUniverseMembership] = []
        for item in snapshot_set.items:
            members = self.store.read_members(item.snapshot_id)
            daily.append(
                DailyUniverseMembership(
                    trading_date=item.trading_date,
                    snapshot_id=item.snapshot_id,
                    instrument_ids=tuple(sorted(member.instrument_id for member in members)),
                )
            )
        return ResolvedUniverse(
            universe_ref=universe_ref,
            universe_id=snapshot_set.universe_id,
            universe_version=snapshot_set.universe_version,
            asset_class=definition.asset_class,
            calendar_id=definition.calendar_id,
            definition_hash=snapshot_set.definition_hash,
            snapshot_set_hash=snapshot_set.snapshot_set_hash,
            daily_memberships=tuple(daily),
        )
