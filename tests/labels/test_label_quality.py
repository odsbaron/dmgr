from quant_research.features.quality import QualitySeverity, QualityStatus
from quant_research.labels.contracts import LabelCommitRequest, LabelValue
from quant_research.labels.duckdb_store import LocalDuckDBLabelStore
from quant_research.labels.quality import LabelQualityAnalyzer


def label_value(index: int, value_float: float | None) -> LabelValue:
    return LabelValue(
        label_run_id="label-run-1",
        label_set_id="next_return_v1",
        dataset_id="fixture-daily",
        symbol="000001.SZ",
        freq="1d",
        as_of=f"2026-07-0{index + 1}T07:00:00+00:00",
        label_id="forward_ret_1",
        label_version="1.0.0",
        value_float=value_float,
        value_string=None,
        value_kind="null" if value_float is None else "float",
        forward_bars=1,
        source_factor_run_id="factor-run-forward",
        created_at="2026-07-08T00:00:00+00:00",
    )


def test_label_quality_analyzer_reports_null_ratio_and_coverage_metrics():
    report = LabelQualityAnalyzer(max_null_ratio=0.5).analyze(
        (label_value(0, 0.01), label_value(1, -0.02), label_value(2, None))
    )

    metrics = {metric.metric_name: metric for metric in report.metrics}
    assert report.status == QualityStatus.PASSED
    assert metrics["row_count"].metric_value == 3
    assert metrics["null_ratio"].metric_value == 1 / 3
    assert metrics["null_ratio"].severity == QualitySeverity.INFO
    assert metrics["symbol_count"].metric_value == 1
    assert metrics["as_of_min"].metric_json["value"] == "2026-07-01T07:00:00+00:00"
    assert metrics["as_of_max"].metric_json["value"] == "2026-07-03T07:00:00+00:00"


def test_label_quality_analyzer_fails_when_null_ratio_exceeds_threshold():
    report = LabelQualityAnalyzer(max_null_ratio=0.2).analyze(
        (label_value(0, 0.01), label_value(1, None))
    )

    null_ratio = [metric for metric in report.metrics if metric.metric_name == "null_ratio"][0]
    assert report.status == QualityStatus.FAILED
    assert null_ratio.severity == QualitySeverity.ERROR


def test_label_store_persists_quality_metrics_and_updates_manifest(tmp_path):
    store = LocalDuckDBLabelStore(tmp_path / "research.duckdb")
    ref = store.commit_labels(
        LabelCommitRequest(
            label_run_id="label-run-1",
            label_set_id="next_return_v1",
            source_factor_run_id="factor-run-forward",
            labels=(label_value(0, 0.01), label_value(1, None)),
        )
    )
    labels = store.read_labels(ref)
    report = LabelQualityAnalyzer(max_null_ratio=0.6).analyze(tuple(labels))

    store.commit_quality_report(report)

    manifest = store.get_manifest("label-run-1")
    metrics = store.list_quality_metrics("label-run-1")
    assert manifest.quality_status == QualityStatus.PASSED.value
    assert manifest.quality_summary["metric_count"] == len(metrics)
    assert {metric.metric_name for metric in metrics} >= {"row_count", "null_ratio"}
