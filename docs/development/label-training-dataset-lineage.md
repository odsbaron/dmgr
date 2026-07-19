# Label and Training-Dataset Lineage

## Label source identity

New label runs use two authoritative fields:

```text
source_kind = MARKET_DATA | FACTOR_RUN | LEGACY
source_ref  = complete DuckDB ref for that source
```

`source_factor_run_id` remains in storage for compatibility with existing databases. For market-data-derived labels it may contain the historical source id and must not be interpreted as a factor-run identity; consumers use `source_kind` and `source_ref` instead.

When `LabelPipeline` receives an exact market-data snapshot-set ref, it resolves that ref before reading bars. The label manifest then records:

```text
dataset_id
freq
forward_bars
source_as_of_start / source_as_of_end
market_data_ref
market_dataset_version
market_data_definition_hash
market_data_snapshot_set_hash
```

Legacy curated-bar refs remain accepted and leave exact market-data lineage nullable. Existing label tables receive additive columns and are backfilled as `LEGACY` without deleting rows.

## Feature/label compatibility

`TrainingFeatureMatrixBuilder` quality-gates both inputs and compares their manifests before joining. It rejects conflicts in:

- dataset id;
- frequency;
- non-null market-data definition hashes;
- explicitly declared Universe lineage;
- label source range relative to feature observations.

Feature and label snapshot-set hashes are not required to match. A forward label normally needs market data beyond the feature output range, so the compatible relationship is a shared dataset definition plus sufficient label-source coverage.

Feature snapshots are the authoritative population. Extra label rows cannot introduce new instruments or timestamps into the training matrix.

## Manifested assembly

Configure a `LocalDuckDBTrainingDatasetStore` and call `build_manifested` to persist a reproducible assembly:

```python
result = TrainingFeatureMatrixBuilder(
    FeatureQualityGate(feature_store),
    LabelQualityGate(label_store),
    LocalDuckDBTrainingDatasetStore(db_path),
).build_manifested(
    "ashare-minute-training-v1",
    feature_snapshot_ref,
    label_ref,
    feature_fields=("ret_1", "volatility_20"),
    label_fields=("forward_ret_5",),
)
```

The `training_dataset_manifest` table records both refs, selected fields, inherited market-data and Universe lineage, factor/label run ids, joined rows, feature-only rows, label-only rows, and a deterministic content hash. Reusing an id with identical content is idempotent; reusing it with different content is rejected.

The matrix is still returned as a Polars `LazyFrame`. Materialized Parquet datasets and expected-minute completeness are separate future changes.
