"""
Tests for transformation/served_export.py, reading curated Delta tables
written by merge_into_curated() into pytest's tmp_path.
"""
import pytest
import shutil

from datetime import datetime, date
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from transformation.curated_writer import merge_into_curated
from transformation.schemas.source_schemas import get_source_schema
from transformation.served_export import read_snapshot
from transformation.served_export import read_changes, read_snapshot, write_served, curated_state



EARLIER = datetime(2025, 6, 1, 9, 0)
LATER = datetime(2025, 6, 10, 9, 0)
LATEST = datetime(2025, 6, 20, 9, 0)
RUN_DATE = date(2026, 10, 1)
EXPORT_ID = "export-1"
EXPORT_DIR = "customers/year=2026/month=10/day=01/export_id=export-1/"


def _customers(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Customer rows in source shape; missing columns become NULL."""
    return spark.createDataFrame(rows, schema=get_source_schema("customers"))


def test_snapshot_reads_the_pinned_version(spark: SparkSession, tmp_path: Path) -> None:
    """A snapshot at version 0 ignores the update committed at version 1."""
    curated_root = str(tmp_path)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER},
    ]), "customers", curated_root)  # version 0
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "new@example.com", "updated_at": LATER},
    ]), "customers", curated_root)  # version 1

    snapshot = read_snapshot(spark, curated_root, "customers", version=0)

    assert [row["email"] for row in snapshot.collect()] == ["old@example.com"]

def test_changes_keep_the_new_state_only(spark: SparkSession, tmp_path: Path) -> None:
    """An update is exported as its postimage; the preimage is not."""
    curated_root = str(tmp_path)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER},
    ]), "customers", curated_root)  # version 0
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "new@example.com", "updated_at": LATER},
    ]), "customers", curated_root)  # version 1

    changes = read_changes(spark, curated_root, "customers", start_version=1, end_version=1)

    assert [row["email"] for row in changes.collect()] == ["new@example.com"]


def test_changes_stop_at_the_end_version(spark: SparkSession, tmp_path: Path) -> None:
    """A version committed after end_version is not exported."""
    curated_root = str(tmp_path)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER},
    ]), "customers", curated_root)  # version 0
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "new@example.com", "updated_at": LATER},
    ]), "customers", curated_root)  # version 1
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "newest@example.com", "updated_at": LATEST},
    ]), "customers", curated_root)  # version 2

    changes = read_changes(spark, curated_root, "customers", start_version=1, end_version=1)

    assert [row["email"] for row in changes.collect()] == ["new@example.com"]

def test_both_reads_return_source_columns_only(spark: SparkSession, tmp_path: Path) -> None:
    """No _change_type, _commit_version or _commit_timestamp reaches served."""
    curated_root = str(tmp_path)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER},
    ]), "customers", curated_root)  # version 0

    expected = get_source_schema("customers").fieldNames()

    assert read_snapshot(spark, curated_root, "customers", version=0).columns == expected
    assert read_changes(spark, curated_root, "customers", start_version=0, end_version=0).columns == expected

def test_key_changed_twice_appears_twice(spark: SparkSession, tmp_path: Path) -> None:
    """Served is a change log: each state of a key in the range is exported."""
    curated_root = str(tmp_path)
    for email, updated_at in [
        ("old@example.com", EARLIER),
        ("new@example.com", LATER),
        ("newest@example.com", LATEST),
    ]:
        merge_into_curated(spark, _customers(spark, [
            {"customer_id": 1, "email": email, "updated_at": updated_at},
        ]), "customers", curated_root)  # versions 0, 1, 2

    changes = read_changes(spark, curated_root, "customers", start_version=1, end_version=2)

    assert sorted(row["email"] for row in changes.collect()) == [
        "new@example.com",
        "newest@example.com",
    ]

def test_written_files_are_relative_and_inside_the_export_directory(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The manifest names files the stage can resolve, from this run only."""
    df = _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER},
        {"customer_id": 2, "email": "b@example.com", "updated_at": EARLIER},
    ])

    files, row_count = write_served(spark, df, str(tmp_path), "customers", RUN_DATE, EXPORT_ID)

    assert row_count == 2
    assert files
    assert all(f.startswith(EXPORT_DIR) and f.endswith(".parquet") for f in files)


def test_empty_export_lists_no_files(spark: SparkSession, tmp_path: Path) -> None:
    """A run with no rows records row_count 0 and no files, so COPY never sees them."""
    empty = _customers(spark, [])

    files, row_count = write_served(spark, empty, str(tmp_path), "customers", RUN_DATE, EXPORT_ID)

    assert (files, row_count) == ([], 0)


def test_reused_export_id_fails_and_keeps_the_first_files(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Written files are immutable: a second write to the same export directory fails."""
    df = _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER},
    ])
    first_files, _ = write_served(spark, df, str(tmp_path), "customers", RUN_DATE, EXPORT_ID)

    with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
        write_served(spark, df, str(tmp_path), "customers", RUN_DATE, EXPORT_ID)

    export_dir = tmp_path / EXPORT_DIR
    assert sorted(p.name for p in export_dir.glob("*.parquet")) == [
        f.rsplit("/", 1)[-1] for f in first_files
    ]


def test_curated_state_is_the_latest_version_and_a_stable_id(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The version moves with every commit; the table id does not."""
    curated_root = str(tmp_path)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER},
    ]), "customers", curated_root)  # version 0
    version_0, id_0 = curated_state(spark, curated_root, "customers")

    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "new@example.com", "updated_at": LATER},
    ]), "customers", curated_root)  # version 1
    version_1, id_1 = curated_state(spark, curated_root, "customers")

    assert (version_0, version_1) == (0, 1)
    assert id_0 == id_1


def test_rebuilt_curated_table_gets_a_new_id(spark: SparkSession, tmp_path: Path) -> None:
    """A rebuild restarts versions under a new id, which is what plan_export detects."""
    curated_root = str(tmp_path)
    rows = _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER},
    ])
    merge_into_curated(spark, rows, "customers", curated_root)
    merge_into_curated(spark, rows, "customers", curated_root)  # version 1, no changes
    _, old_id = curated_state(spark, curated_root, "customers")

    shutil.rmtree(tmp_path / "customers")  # the rebuild: folder gone, table written again
    merge_into_curated(spark, rows, "customers", curated_root)
    new_version, new_id = curated_state(spark, curated_root, "customers")

    assert new_version == 0
    assert new_id != old_id
