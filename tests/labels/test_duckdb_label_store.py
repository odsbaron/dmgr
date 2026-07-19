import pytest

from quant_research.labels.contracts import LabelCommitRequest, LabelStoreError, LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore


def label_value(
    index: int,
    value_float: float | None,
    *,
    label_id: str = "forward_ret_1",
) -> LabelValue:
    return LabelValue(
        label_run_id="label-run-1",
        label_set_id="next_return_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        label_id=label_id,
        label_version="1.0.0",
        value_float=value_float,
        value_string=None,
        value_kind="null" if value_float is None else "float",
        forward_bars=1,
        source_factor_run_id="factor-run-forward",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_label_store_commits_manifest_and_reads_labels(tmp_path):
    store = LocalDuckDBLabelStore(tmp_path / "research.duckdb")

    ref = store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="factor-run-forward",
            labels=(label_value(0, 0.01), label_value(1, None)),
        )
    )

    rows = store.read_labels(ref)
    manifest = store.get_manifest("label-run-1")

    assert ref.table == "label_table"
    assert ref.filters == {"label_run_id": "label-run-1"}
    assert len(rows) == 2
    assert rows[0].value == 0.01
    assert rows[1].value is None
    assert manifest is not None
    assert manifest.label_set_id == "next_return_v1"
    assert manifest.source_factor_run_id == "factor-run-forward"
    assert manifest.row_count_label == 2
    assert manifest.status == "COMMITTED"


def test_label_store_rejects_duplicate_label_key(tmp_path):
    store = LocalDuckDBLabelStore(tmp_path / "research.duckdb")

    with pytest.raises(LabelStoreError) as exc_info:
        store.commit_labels(
            LabelCommitRequest(
                label_run_id="label-run-1",
                label_set_id="next_return_v1",
                source_factor_run_id="factor-run-forward",
                labels=(label_value(0, 0.01), label_value(0, 0.02)),
            )
        )

    assert exc_info.value.code == "DUPLICATE_LABEL_KEY"
    assert store.get_manifest("label-run-1") is None


def test_label_store_reports_duplicate_label_key_code(tmp_path):
    store = LocalDuckDBLabelStore(tmp_path / "research.duckdb")

    try:
        store.validate_labels((label_value(0, 0.01), label_value(0, 0.02)))
    except LabelStoreError as exc:
        assert exc.code == "DUPLICATE_LABEL_KEY"
    else:
        raise AssertionError("expected duplicate label key to fail")
