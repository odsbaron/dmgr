from __future__ import annotations

from dataclasses import dataclass

from quant_research.data.duckdb_store import LocalDuckDBStore, MarketDataStoreError
from quant_research.data.partition_contracts import (
    MarketDataRef,
    ResolvedMarketData,
)


@dataclass(frozen=True)
class MarketDataResolver:
    store: LocalDuckDBStore

    def resolve(self, ref: MarketDataRef | str) -> ResolvedMarketData:
        market_data_ref = MarketDataRef.parse(ref)
        snapshot_set = self.store.read_market_data_snapshot_set(market_data_ref)
        definition = self.store.get_market_dataset_definition(
            snapshot_set.dataset_id,
            snapshot_set.dataset_version,
        )
        if definition is None:
            raise MarketDataStoreError(
                "UNKNOWN_DATASET_DEFINITION",
                "market-data snapshot set references an unknown definition",
            )
        if definition.definition_hash != snapshot_set.definition_hash:
            raise MarketDataStoreError(
                "DEFINITION_HASH_MISMATCH",
                "market-data snapshot set definition hash does not match definition",
            )
        for item in snapshot_set.items:
            partition = self.store.get_market_data_partition(item.partition_id)
            if partition is None:
                raise MarketDataStoreError(
                    "UNKNOWN_PARTITION",
                    f"market-data snapshot set references unknown partition: {item.partition_id}",
                )
            if (
                partition.dataset_id != snapshot_set.dataset_id
                or partition.dataset_version != snapshot_set.dataset_version
                or partition.trading_date != item.trading_date
                or partition.definition_hash != snapshot_set.definition_hash
            ):
                raise MarketDataStoreError(
                    "PARTITION_IDENTITY_MISMATCH",
                    f"market-data partition identity mismatch: {item.partition_id}",
                )
            if partition.content_hash != item.content_hash:
                raise MarketDataStoreError(
                    "PARTITION_HASH_MISMATCH",
                    f"market-data partition hash mismatch: {item.partition_id}",
                )
        return ResolvedMarketData(
            market_data_ref=market_data_ref,
            dataset_id=snapshot_set.dataset_id,
            dataset_version=snapshot_set.dataset_version,
            asset_class=definition.asset_class,
            freq=definition.freq,
            adjustment=definition.adjustment,
            calendar_id=definition.calendar_id,
            timezone=definition.timezone,
            definition_hash=snapshot_set.definition_hash,
            snapshot_set_hash=snapshot_set.snapshot_set_hash,
            items=snapshot_set.items,
        )
