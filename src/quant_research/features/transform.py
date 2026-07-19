from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from quant_research.contracts.refs import DataRef
from quant_research.features.contracts import (
    FeatureCommitRequest,
    FeatureSnapshot,
    FeatureStoreError,
    FeatureValue,
)


_KEY_COLUMNS = ("dataset_id", "symbol", "freq", "as_of")


def wide_to_feature_values(request: FeatureCommitRequest) -> list[FeatureValue]:
    output_fields = _declared_output_fields(request)
    _validate_no_duplicate_outputs(output_fields)
    required = set(_KEY_COLUMNS) | {output for _, _, output in output_fields}
    columns = set(request.factor_frame.collect_schema().names())
    missing = sorted(required - columns)
    if missing:
        code = "MISSING_KEY_COLUMN" if set(_KEY_COLUMNS) & set(missing) else "MISSING_FACTOR_OUTPUT"
        raise FeatureStoreError(code, f"missing feature frame columns: {', '.join(missing)}")

    frame = request.factor_frame.select(sorted(required)).sort(["symbol", "as_of"]).collect()
    if frame.height == 0:
        raise FeatureStoreError("EMPTY_FACTOR_FRAME", "factor frame has zero rows")

    created_at = datetime.now(UTC).isoformat()
    values: list[FeatureValue] = []
    symbol_index: defaultdict[str, int] = defaultdict(int)
    for row in frame.to_dicts():
        symbol = str(row["symbol"])
        row_index = symbol_index[symbol]
        symbol_index[symbol] += 1
        for registered, factor_id, output_field in output_fields:
            raw_value = row[output_field]
            value_float, value_string, value_kind = _split_value(raw_value)
            values.append(
                FeatureValue(
                    factor_run_id=request.config.factor_run_id,
                    feature_set_id=request.config.feature_set_id,
                    dataset_id=str(row["dataset_id"]),
                    symbol=symbol,
                    freq=str(row["freq"]),
                    as_of=_format_as_of(row["as_of"]),
                    factor_id=factor_id,
                    factor_version=registered.spec.version,
                    output_field=output_field,
                    value_float=value_float,
                    value_string=value_string,
                    value_kind=value_kind,
                    warmup_complete=row_index >= registered.spec.warmup_bars,
                    quality_flags=(),
                    input_data_ref=request.config.input_data_ref,
                    created_at=created_at,
                    trading_date=_format_as_of(row["as_of"])[:10],
                )
            )
    _validate_unique_feature_keys(values)
    return values


def build_feature_snapshots(
    config,
    values: list[FeatureValue],
) -> list[FeatureSnapshot]:
    groups: dict[tuple[str, str, str, str, str], list[FeatureValue]] = defaultdict(list)
    for value in values:
        key = (value.feature_set_id, value.dataset_id, value.symbol, value.freq, value.as_of)
        groups[key].append(value)

    snapshots: list[FeatureSnapshot] = []
    for key in sorted(groups):
        feature_set_id, dataset_id, symbol, freq, as_of = key
        group = groups[key]
        features: dict[str, object] = {}
        for value in group:
            if value.output_field in features:
                raise FeatureStoreError(
                    "DUPLICATE_OUTPUT_FIELD",
                    f"duplicate output field in snapshot: {value.output_field}",
                )
            features[value.output_field] = value.value
        factor_run_ids = tuple(sorted({value.factor_run_id for value in group}))
        input_data_refs = tuple(sorted({value.input_data_ref for value in group}))
        flags = tuple(sorted({flag for value in group for flag in value.quality_flags}))
        feature_ref = DataRef(
            "feature_snapshot",
            {
                "feature_set_id": feature_set_id,
                "factor_run_id": config.factor_run_id,
                "symbol": symbol,
                "freq": freq,
                "as_of": as_of,
            },
        ).uri
        snapshots.append(
            FeatureSnapshot(
                snapshot_id=":".join(
                    [feature_set_id, dataset_id, symbol, freq, as_of, config.factor_run_id]
                ),
                feature_set_id=feature_set_id,
                dataset_id=dataset_id,
                symbol=symbol,
                freq=freq,
                as_of=as_of,
                features=features,
                factor_run_ids=factor_run_ids,
                input_data_refs=input_data_refs,
                warmup_complete=all(value.warmup_complete for value in group),
                quality_flags=flags,
                feature_ref=feature_ref,
                created_at=group[0].created_at,
            )
        )
    return snapshots


def _declared_output_fields(request: FeatureCommitRequest):
    fields = []
    for registered in request.resolved_factors:
        for output_field in registered.spec.output_fields:
            fields.append((registered, registered.spec.factor_id, output_field))
    return fields


def _validate_no_duplicate_outputs(output_fields) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for _, _, output_field in output_fields:
        if output_field in seen:
            duplicates.add(output_field)
        seen.add(output_field)
    if duplicates:
        raise FeatureStoreError(
            "DUPLICATE_OUTPUT_FIELD",
            f"duplicate output fields: {', '.join(sorted(duplicates))}",
        )


def _validate_unique_feature_keys(values: list[FeatureValue]) -> None:
    seen: set[tuple[str, str, str, str, str, str, str, str]] = set()
    for value in values:
        key = (
            value.feature_set_id,
            value.dataset_id,
            value.symbol,
            value.freq,
            value.as_of,
            value.factor_id,
            value.factor_version,
            value.output_field,
        )
        if key in seen:
            raise FeatureStoreError("DUPLICATE_FEATURE_KEY", "duplicate feature key")
        seen.add(key)


def _format_as_of(value: object) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _split_value(value: Any) -> tuple[float | None, str | None, str]:
    if value is None:
        return None, None, "null"
    if isinstance(value, bool):
        return None, "true" if value else "false", "bool"
    if isinstance(value, int | float):
        return float(value), None, "float"
    return None, str(value), "string"
