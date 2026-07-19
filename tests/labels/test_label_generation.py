from datetime import UTC, date, datetime, timedelta

import pytest

from quant_research.contracts.bar import Adjustment, AssetClass, BarRecord, Frequency
from quant_research.features.contracts import FeatureValue
from quant_research.labels.generation import (
    ForwardReturnLabelConfig,
    feature_values_to_label_request,
    forward_return_labels_from_bars,
)
from quant_research.labels.contracts import LabelSourceKind


def bar(close: str, index: int, *, symbol: str = "000001.SZ") -> BarRecord:
    timestamp = datetime(2026, 7, 1, 7, 0, tzinfo=UTC) + timedelta(days=index)
    return BarRecord(
        dataset_id="fixture-daily",
        symbol=symbol,
        exchange="XSHE",
        asset_class=AssetClass.EQUITY,
        freq=Frequency.D1,
        trading_date=date(2026, 7, 1 + index),
        bar_start_time=timestamp,
        bar_end_time=timestamp,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1000",
        turnover="10000",
        adjustment=Adjustment.NONE,
        source="csv",
        source_run_id="import-run-1",
        source_row_id=f"row-{index}",
        raw_ref="fixture.csv",
    )


def feature_value(index: int, value: float | None) -> FeatureValue:
    return FeatureValue(
        factor_run_id="factor-run-forward",
        feature_set_id="label_probe_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        factor_id="forward_ret_1",
        factor_version="1.0.0",
        output_field="forward_ret_1",
        value_float=value,
        value_string=None,
        value_kind="null" if value is None else "float",
        warmup_complete=True,
        quality_flags=(),
        input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_forward_return_labels_from_bars_align_to_current_bar_as_of():
    request = forward_return_labels_from_bars(
        [bar("10.0", 0), bar("11.0", 1), bar("12.1", 2)],
        ForwardReturnLabelConfig(
            label_run_id="label-run-bars",
            label_set_id="next_return_v1",
            label_id="forward_ret_1",
            label_version="1.0.0",
            forward_bars=1,
            source_id="curated-bars",
        ),
    )

    values = request.labels
    assert request.source_factor_run_id == "curated-bars"
    assert [label.as_of for label in values] == [
        "2026-07-01T07:00:00+00:00",
        "2026-07-02T07:00:00+00:00",
        "2026-07-03T07:00:00+00:00",
    ]
    assert [label.value_float for label in values] == pytest.approx([0.1, 0.1, None])
    assert values[-1].value_kind == "null"


def test_forward_return_labels_are_symbol_local():
    request = forward_return_labels_from_bars(
        [
            bar("10.0", 0, symbol="000001.SZ"),
            bar("20.0", 0, symbol="000002.SZ"),
            bar("11.0", 1, symbol="000001.SZ"),
            bar("18.0", 1, symbol="000002.SZ"),
        ],
        ForwardReturnLabelConfig(
            label_run_id="label-run-bars",
            label_set_id="next_return_v1",
            label_id="forward_ret_1",
            label_version="1.0.0",
            forward_bars=1,
            source_id="curated-bars",
        ),
    )

    by_symbol = {(label.symbol, label.as_of): label.value_float for label in request.labels}
    assert by_symbol[("000001.SZ", "2026-07-01T07:00:00+00:00")] == pytest.approx(0.1)
    assert by_symbol[("000002.SZ", "2026-07-01T07:00:00+00:00")] == pytest.approx(-0.1)


def test_feature_values_to_label_request_preserves_forward_factor_lineage():
    request = feature_values_to_label_request(
        (feature_value(0, 0.02), feature_value(1, None)),
        label_run_id="label-run-forward",
        label_set_id="next_return_v1",
        forward_bars=1,
    )

    assert request.label_run_id == "label-run-forward"
    assert request.source_factor_run_id == "factor-run-forward"
    assert request.source_kind == LabelSourceKind.FACTOR_RUN
    assert request.source_ref == (
        "duckdb://factor_run_manifest?factor_run_id=factor-run-forward"
    )
    assert [label.label_id for label in request.labels] == ["forward_ret_1", "forward_ret_1"]
    assert [label.value for label in request.labels] == [0.02, None]
