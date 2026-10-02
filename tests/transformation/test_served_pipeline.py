"""
Tests for transformation/served_pipeline.py: the whole export of one table,
on real Delta tables in pytest's tmp_path.
"""

from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from transformation.curated_writer import merge_into_curated, curated_path
from transformation.schemas.source_schemas import get_source_schema
from transformation.served_manifest import MANIFEST_DIRECTORY, SNAPSHOT_MODE, CDF_MODE, manifest_path
from transformation.served_pipeline import export_table, parse_flag
from transformation.served_export import curated_state

TABLE = "customers"
RUN_ID = "airflow-run-1"
RUN_DATE = date(2026, 10, 1)
UPDATED = datetime(2025, 6, 1, 9, 0)
WRITTEN = datetime(2026, 10, 1, 9, 0)
UPDATED_LATER = datetime(2025, 6, 10, 9, 0)
NEXT_RUN_DATE = date(2026, 10, 2)
WRITTEN_NEXT_DAY = datetime(2026, 10, 2, 9, 0)


def _customers(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Customer rows in source shape; missing columns become NULL."""
    return spark.createDataFrame(rows, schema=get_source_schema(TABLE))


def test_first_export_without_full_reload_fails_and_writes_nothing(
    spark: SparkSession, tmp_path: Path
) -> None:
    """No history and no flag: the run stops before served or the manifest is touched."""
    curated_root = str(tmp_path / "curated")
    served_root = str(tmp_path / "served")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)

    with pytest.raises(ValueError, match="full_reload"):
        export_table(
            spark, curated_root, served_root, TABLE,
            run_id=RUN_ID, run_date=RUN_DATE, full_reload=False,
            written_at=WRITTEN, export_id="export-1",
        )

    assert not (tmp_path / "served").exists()
    assert not (tmp_path / "curated" / MANIFEST_DIRECTORY).exists()


def test_full_reload_exports_every_row_as_a_snapshot(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The first export writes the whole table and one snapshot row to the manifest."""
    curated_root = str(tmp_path / "curated")
    served_root = str(tmp_path / "served")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
        {"customer_id": 2, "email": "b@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)  # version 0

    summary = export_table(
        spark, curated_root, served_root, TABLE,
        run_id=RUN_ID, run_date=RUN_DATE, full_reload=True,
        written_at=WRITTEN, export_id="export-1",
    )

    assert summary["export_mode"] == SNAPSHOT_MODE
    assert (summary["start_version"], summary["end_version"]) == (None, 0)
    assert summary["row_count"] == 2

    manifest = spark.read.format("delta").load(manifest_path(curated_root)).collect()
    assert len(manifest) == 1
    assert manifest[0]["run_id"] == RUN_ID
    assert manifest[0]["row_count"] == 2
    assert spark.read.parquet(f"{served_root}/{manifest[0]['files'][0]}").count() > 0


def test_next_run_exports_only_the_changes(spark: SparkSession, tmp_path: Path) -> None:
    """After a snapshot at version 0, a MERGE at version 1 is exported as a cdf range 1 to 1."""
    curated_root = str(tmp_path / "curated")
    served_root = str(tmp_path / "served")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
        {"customer_id": 2, "email": "b@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)  # version 0
    export_table(
        spark, curated_root, served_root, TABLE,
        run_id=RUN_ID, run_date=RUN_DATE, full_reload=True,
        written_at=WRITTEN, export_id="export-1",
    )

    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a.new@example.com", "updated_at": UPDATED_LATER},
        {"customer_id": 3, "email": "c@example.com", "updated_at": UPDATED_LATER},
    ]), TABLE, curated_root)  # version 1: one update, one insert

    summary = export_table(
        spark, curated_root, served_root, TABLE,
        run_id="airflow-run-2", run_date=NEXT_RUN_DATE, full_reload=False,
        written_at=WRITTEN_NEXT_DAY, export_id="export-2",
    )

    assert summary["export_mode"] == CDF_MODE
    assert (summary["start_version"], summary["end_version"]) == (1, 1)
    assert summary["row_count"] == 2

    manifest = spark.read.format("delta").load(manifest_path(curated_root))
    second = manifest.where(manifest.run_id == "airflow-run-2").first()
    exported = spark.read.parquet(*[f"{served_root}/{f}" for f in second["files"]])
    assert sorted(row["email"] for row in exported.collect()) == [
        "a.new@example.com",
        "c@example.com",
    ]


def test_run_with_no_new_versions_does_nothing(spark: SparkSession, tmp_path: Path) -> None:
    """No commit since the last export: no read, no files, no manifest row."""
    curated_root = str(tmp_path / "curated")
    served_root = str(tmp_path / "served")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)  # version 0
    export_table(
        spark, curated_root, served_root, TABLE,
        run_id=RUN_ID, run_date=RUN_DATE, full_reload=True,
        written_at=WRITTEN, export_id="export-1",
    )

    summary = export_table(
        spark, curated_root, served_root, TABLE,
        run_id="airflow-run-2", run_date=NEXT_RUN_DATE, full_reload=False,
        written_at=WRITTEN_NEXT_DAY, export_id="export-2",
    )

    assert summary is None
    assert spark.read.format("delta").load(manifest_path(curated_root)).count() == 1
    assert not list((tmp_path / "served").rglob("export_id=export-2"))


def test_new_version_without_row_changes_records_an_empty_export(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A metadata-only commit advances the watermark with row_count 0 and no files."""
    curated_root = str(tmp_path / "curated")
    served_root = str(tmp_path / "served")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)  # version 0
    export_table(
        spark, curated_root, served_root, TABLE,
        run_id=RUN_ID, run_date=RUN_DATE, full_reload=True,
        written_at=WRITTEN, export_id="export-1",
    )

    spark.sql(
        f"ALTER TABLE delta.`{curated_path(curated_root, TABLE)}` "
        "SET TBLPROPERTIES ('test.note' = 'metadata only')"
    )  # version 1, no data
    assert curated_state(spark, curated_root, TABLE)[0] == 1

    summary = export_table(
        spark, curated_root, served_root, TABLE,
        run_id="airflow-run-2", run_date=NEXT_RUN_DATE, full_reload=False,
        written_at=WRITTEN_NEXT_DAY, export_id="export-2",
    )

    assert summary == {
        "export_mode": CDF_MODE,
        "start_version": 1,
        "end_version": 1,
        "row_count": 0,
        "file_count": 0,
    }
    assert spark.read.format("delta").load(manifest_path(curated_root)).count() == 2


def test_parse_flag_accepts_true_and_false() -> None:
    """The only two spellings a flag may have."""
    assert parse_flag("full_reload", "true") is True
    assert parse_flag("full_reload", "false") is False


@pytest.mark.parametrize("value", ["ture", "True", "true ", "yes", ""])
def test_parse_flag_rejects_anything_else(value: str) -> None:
    """A typo stops the run instead of silently meaning false."""
    with pytest.raises(ValueError, match="full_reload"):
        parse_flag("full_reload", value)



