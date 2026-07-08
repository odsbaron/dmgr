from datetime import UTC, datetime

import pytest

from quant_research.contracts.bar import Frequency
from quant_research.features.quality import QualityStatus
from quant_research.pipeline.contracts import (
    PipelineInputRefError,
    ResearchRunRequest,
    ResearchRunResult,
    ResearchRunStatus,
    parse_pipeline_input_ref,
)


def request(input_data_ref: str) -> ResearchRunRequest:
    return ResearchRunRequest(
        factor_run_id="factor-run-1",
        feature_set_id="basic_price_v1",
        input_data_ref=input_data_ref,
        factor_ids=("ret_1",),
    )


def test_research_run_request_keeps_dataset_and_freq_inside_data_ref():
    run_request = request("duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d")

    parsed = parse_pipeline_input_ref(run_request)

    assert parsed.data_ref.table == "curated_market_bar"
    assert parsed.dataset_id == "fixture-daily"
    assert parsed.freq == Frequency.D1
    assert not hasattr(run_request, "dataset_id")
    assert not hasattr(run_request, "freq")
    assert not hasattr(run_request, "strict_quality")


def test_research_run_request_validates_run_level_filters():
    with pytest.raises(ValueError, match="symbols must not be empty"):
        ResearchRunRequest(
            factor_run_id="factor-run-1",
            feature_set_id="basic_price_v1",
            input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
            factor_ids=("ret_1",),
            symbols=(),
        )

    with pytest.raises(ValueError, match="as_of_start must be <= as_of_end"):
        ResearchRunRequest(
            factor_run_id="factor-run-1",
            feature_set_id="basic_price_v1",
            input_data_ref="duckdb://curated_market_bar?dataset_id=fixture-daily&freq=1d",
            factor_ids=("ret_1",),
            as_of_start=datetime(2026, 7, 2, tzinfo=UTC),
            as_of_end=datetime(2026, 7, 1, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("input_data_ref", "message"),
    [
        ("file://curated_market_bar?dataset_id=fixture-daily&freq=1d", "unsupported"),
        ("duckdb://feature_table?dataset_id=fixture-daily&freq=1d", "curated_market_bar"),
        ("duckdb://curated_market_bar?freq=1d", "dataset_id"),
        ("duckdb://curated_market_bar?dataset_id=fixture-daily", "freq"),
        ("duckdb://curated_market_bar?dataset_id=fixture-daily&freq=7m", "freq"),
    ],
)
def test_parse_pipeline_input_ref_rejects_invalid_refs(input_data_ref: str, message: str):
    with pytest.raises(PipelineInputRefError, match=message):
        parse_pipeline_input_ref(request(input_data_ref))


def test_research_run_result_marks_non_passed_quality_as_not_consumable():
    result = ResearchRunResult.from_quality_status(
        factor_run_id="factor-run-1",
        status=ResearchRunStatus.QUALITY_FAILED,
        quality_status=QualityStatus.FAILED,
        quality_summary={"status": "FAILED"},
        row_count_input=2,
        row_count_feature=2,
        row_count_snapshot=2,
        metric_count=1,
    )

    assert result.consumable is False
    assert result.block_reason == "quality_failed"
