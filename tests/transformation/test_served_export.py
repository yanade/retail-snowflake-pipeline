"""
Tests for transformation/served_export.py, reading curated Delta tables
written by merge_into_curated() into pytest's tmp_path.
"""

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from transformation.curated_writer import merge_into_curated
from transformation.schemas.source_schemas import get_source_schema
from transformation.served_export import read_snapshot
from transformation.served_export import read_changes, read_snapshot



EARLIER = datetime(2025, 6, 1, 9, 0)
LATER = datetime(2025, 6, 10, 9, 0)
LATEST = datetime(2025, 6, 20, 9, 0)


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