"""Data ingestion readers, normalization, validation, and storage."""

from quant_research.data.duckdb_store import LocalDuckDBStore, MarketDataStoreError
from quant_research.data.ingestion import (
    ImmutableMarketDataIngestionService,
    MarketDataIngestionResult,
)
from quant_research.data.partition_contracts import (
    MarketDataImportRun,
    MarketDataPartition,
    MarketDataRef,
    MarketDataSnapshotSet,
    MarketDataSnapshotSetItem,
    MarketDataSourceSpec,
    MarketDatasetDefinition,
    ResolvedMarketData,
)
from quant_research.data.resolver import MarketDataResolver

__all__ = [
    "ImmutableMarketDataIngestionService",
    "LocalDuckDBStore",
    "MarketDataImportRun",
    "MarketDataIngestionResult",
    "MarketDataPartition",
    "MarketDataRef",
    "MarketDataResolver",
    "MarketDataSnapshotSet",
    "MarketDataSnapshotSetItem",
    "MarketDataSourceSpec",
    "MarketDataStoreError",
    "MarketDatasetDefinition",
    "ResolvedMarketData",
]
