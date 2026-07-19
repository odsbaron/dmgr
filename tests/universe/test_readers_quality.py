from datetime import UTC, date, datetime, time

import polars as pl

from quant_research.contracts.bar import AssetClass
from quant_research.universe.contracts import (
    UniverseDefinition,
    UniverseSnapshot,
    UniverseSourceSpec,
    UniverseSourceType,
)
from quant_research.universe.normalize import normalize_universe_rows
from quant_research.universe.quality import UniverseQualityValidator
from quant_research.universe.readers.csv_reader import CSVUniverseReader
from quant_research.universe.readers.parquet_reader import ParquetUniverseReader


def definition() -> UniverseDefinition:
    return UniverseDefinition(
        universe_id="ashare-research",
        version="v1",
        name="A-share research universe",
        asset_class=AssetClass.EQUITY,
        calendar_id="XSHG_XSHE",
        timezone="Asia/Shanghai",
        selection_cutoff_time=time(9, 30),
    )


def spec(path, source_type: UniverseSourceType, **overrides) -> UniverseSourceSpec:
    params = {
        "source_id": "local-universe",
        "universe_id": "ashare-research",
        "universe_version": "v1",
        "source_type": source_type,
        "path": str(path),
        "trading_date": date(2026, 7, 1),
        "known_at": datetime(2026, 7, 1, 1, 0, tzinfo=UTC),
        "source_data_cutoff": datetime(2026, 6, 30, 7, 0, tzinfo=UTC),
        "field_mapping": {
            "instrument_id": "symbol",
            "trading_date": "trading_date",
            "weight": "weight",
            "rank": "rank",
            "inclusion_tags": "tags",
        },
    }
    params.update(overrides)
    return UniverseSourceSpec(**params)


def test_csv_and_parquet_normalize_to_equivalent_snapshot_content(tmp_path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "symbol,trading_date,weight,rank,tags\n"
        '000001.SZ,2026-07-01,0.4,1,"[""liquid""]"\n'
        '600000.SH,2026-07-01,0.6,2,"[""liquid"",""large""]"\n',
        encoding="utf-8",
    )
    parquet_path = tmp_path / "members.parquet"
    pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600000.SH"],
            "trading_date": [date(2026, 7, 1), date(2026, 7, 1)],
            "weight": [0.4, 0.6],
            "rank": [1, 2],
            "tags": [["liquid"], ["large", "liquid"]],
        }
    ).write_parquet(parquet_path)

    csv_spec = spec(csv_path, UniverseSourceType.CSV)
    parquet_spec = spec(parquet_path, UniverseSourceType.PARQUET)
    csv_members = normalize_universe_rows(
        CSVUniverseReader().read_rows(csv_spec), csv_spec, import_run_id="csv-run"
    )
    parquet_members = normalize_universe_rows(
        ParquetUniverseReader().read_rows(parquet_spec),
        parquet_spec,
        import_run_id="parquet-run",
    )

    assert [member.canonical_payload for member in csv_members] == [
        member.canonical_payload for member in parquet_members
    ]
    csv_snapshot = UniverseSnapshot.create(
        definition(), csv_spec, csv_members, source_file_hash="sha256:csv"
    )
    parquet_snapshot = UniverseSnapshot.create(
        definition(), parquet_spec, parquet_members, source_file_hash="sha256:parquet"
    )
    assert csv_snapshot.content_hash == parquet_snapshot.content_hash


def test_quality_rejects_duplicates_and_partition_date_mismatch(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text(
        "symbol,trading_date,weight,rank,tags\n"
        "000001.SZ,2026-07-01,0.5,1,liquid\n"
        "000001.SZ,2026-07-02,0.5,2,liquid\n",
        encoding="utf-8",
    )
    source = spec(path, UniverseSourceType.CSV)
    members = normalize_universe_rows(
        CSVUniverseReader().read_rows(source), source, import_run_id="run-bad"
    )

    report = UniverseQualityValidator("run-bad").validate(definition(), source, members)

    assert report.has_blocking_errors
    assert {issue.issue_code for issue in report.issues} == {
        "DUPLICATE_MEMBER",
        "PARTITION_DATE_MISMATCH",
    }
    assert len({issue.issue_id for issue in report.issues}) == 2


def test_quality_rejects_empty_and_late_point_in_time_snapshot(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("symbol,trading_date,weight,rank,tags\n", encoding="utf-8")
    source = spec(
        path,
        UniverseSourceType.CSV,
        known_at=datetime(2026, 7, 1, 2, 0, tzinfo=UTC),
        source_data_cutoff=datetime(2026, 7, 1, 2, 30, tzinfo=UTC),
    )

    report = UniverseQualityValidator("run-empty").validate(definition(), source, ())

    assert report.has_blocking_errors
    assert {issue.issue_code for issue in report.issues} == {
        "EMPTY_SNAPSHOT",
        "FUTURE_SOURCE_CUTOFF",
        "LATE_KNOWN_AT",
    }
