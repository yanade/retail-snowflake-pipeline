"""
Tests for transformation/served_reconcile.py: replaying served and comparing
it with curated, on real Delta tables in pytest's tmp_path.
"""

from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from transformation.curated_writer import merge_into_curated
from transformation.schemas.source_schemas import get_source_schema
from transformation.served_pipeline import export_table
from transformation.served_reconcile import reconcile, replay_latest

TABLE = "customers"
RUN_DATE = date(2026, 10, 2)
UPDATED = datetime(2025, 6, 1, 9, 0)
UPDATED_LATER = datetime(2025, 6, 10, 9, 0)


def _customers(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Customer rows in source shape; missing columns become NULL."""
    return spark.createDataFrame(rows, schema=get_source_schema(TABLE))


def _export(spark: SparkSession, tmp_path: Path, full_reload: bool, attempt: int) -> None:
    """Export customers to served, as one run of 02_curated_to_served would."""
    export_table(
        spark, str(tmp_path / "curated"), str(tmp_path / "served"), TABLE,
        run_id=f"run-{attempt}", run_date=RUN_DATE, full_reload=full_reload,
        written_at=datetime(2026, 10, 2, 9, attempt), export_id=f"export-{attempt}",
    )


def _reconcile(spark: SparkSession, tmp_path: Path) -> dict[str, int]:
    """Reconcile customers between the tmp_path zones."""
    return reconcile(spark, str(tmp_path / "curated"), str(tmp_path / "served"), TABLE)


def test_replay_latest_keeps_the_newest_row_per_key(spark: SparkSession) -> None:
    """A key exported twice comes back once, in its newer state."""
    served = spark.createDataFrame(
        [(1, "old", UPDATED), (1, "new", UPDATED_LATER), (2, "only", UPDATED)],
        "id INT, value STRING, updated_at TIMESTAMP",
    )

    rows = {r["id"]: r["value"] for r in replay_latest(served, ["id"]).collect()}

    assert rows == {1: "new", 2: "only"}


def test_replay_latest_partitions_by_every_key_column(spark: SparkSession) -> None:
    """Rows sharing only part of a composite key are different keys."""
    served = spark.createDataFrame(
        [(1, 1, UPDATED), (1, 2, UPDATED), (1, 2, UPDATED_LATER)],
        "order_id INT, line INT, updated_at TIMESTAMP",
    )

    latest = replay_latest(served, ["order_id", "line"])

    assert latest.count() == 2


def test_reconcile_matches_after_a_snapshot_and_changes(spark: SparkSession, tmp_path: Path) -> None:
    """Snapshot plus one cdf export rebuild curated, including an updated key."""
    curated_root = str(tmp_path / "curated")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
        {"customer_id": 2, "email": "b@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)
    _export(spark, tmp_path, full_reload=True, attempt=1)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a.new@example.com", "updated_at": UPDATED_LATER},
        {"customer_id": 3, "email": "c@example.com", "updated_at": UPDATED_LATER},
    ]), TABLE, curated_root)
    _export(spark, tmp_path, full_reload=False, attempt=2)

    result = _reconcile(spark, tmp_path)

    assert result == {
        "served_rows": 4,  # 2 from the snapshot, 2 from cdf
        "latest_per_key": 3,
        "curated_rows": 3,
        "only_in_served": 0,
        "only_in_curated": 0,
    }


def test_reconcile_counts_a_row_never_exported(spark: SparkSession, tmp_path: Path) -> None:
    """A curated insert with no export afterwards shows up on the curated side only."""
    curated_root = str(tmp_path / "curated")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)
    _export(spark, tmp_path, full_reload=True, attempt=1)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 2, "email": "b@example.com", "updated_at": UPDATED_LATER},
    ]), TABLE, curated_root)

    result = _reconcile(spark, tmp_path)

    assert (result["only_in_served"], result["only_in_curated"]) == (0, 1)


def test_reconcile_counts_an_update_never_exported(spark: SparkSession, tmp_path: Path) -> None:
    """A stale value in served shows up on both sides: old in served, new in curated."""
    curated_root = str(tmp_path / "curated")
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, curated_root)
    _export(spark, tmp_path, full_reload=True, attempt=1)
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a.new@example.com", "updated_at": UPDATED_LATER},
    ]), TABLE, curated_root)

    result = _reconcile(spark, tmp_path)

    assert (result["only_in_served"], result["only_in_curated"]) == (1, 1)


def test_reconcile_without_any_export_fails(spark: SparkSession, tmp_path: Path) -> None:
    """No manifest files means nothing to compare, which must not read as a pass."""
    merge_into_curated(spark, _customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": UPDATED},
    ]), TABLE, str(tmp_path / "curated"))

    with pytest.raises(ValueError, match="export it first"):
        _reconcile(spark, tmp_path)
