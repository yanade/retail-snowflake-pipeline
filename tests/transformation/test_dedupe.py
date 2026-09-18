"""
Tests for transformation/dedupe.py. Only the key, the watermark and
_source_file matter to dedupe, so rows are built with just those columns.
"""

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from transformation.dedupe import dedupe

EARLIER = datetime(2025, 1, 1, 10, 0)
LATER = datetime(2025, 1, 2, 10, 0)
ROW_SCHEMA = "customer_id long, email string, updated_at timestamp, _source_file string"


def _customers(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Customer rows as (customer_id, email, updated_at, _source_file)."""
    return spark.createDataFrame(rows, ROW_SCHEMA)


def test_latest_version_wins(spark: SparkSession) -> None:
    """A late-arriving update replaces the earlier version."""
    df = _customers(spark, [
        (1, "old@example.com", EARLIER, "run_a.json"),
        (1, "new@example.com", LATER, "run_b.json"),
    ])

    rows = dedupe(df, "customers").collect()

    assert len(rows) == 1
    assert rows[0]["email"] == "new@example.com"


def test_different_keys_are_all_kept(spark: SparkSession) -> None:
    """Dedupe only collapses rows that share a primary key."""
    df = _customers(spark, [
        (1, "a@example.com", EARLIER, "run_a.json"),
        (2, "b@example.com", EARLIER, "run_a.json"),
    ])

    assert dedupe(df, "customers").count() == 2


def test_overlap_copies_collapse_to_one(spark: SparkSession) -> None:
    """The same version copied by two overlapping ADF windows becomes one row."""
    df = _customers(spark, [
        (1, "a@example.com", EARLIER, "run_a.json"),
        (1, "a@example.com", EARLIER, "run_b.json"),
    ])

    assert dedupe(df, "customers").count() == 1


def test_tie_break_is_deterministic(spark: SparkSession) -> None:
    """Same key and timestamp: the result depends on the data, not on luck."""
    df = _customers(spark, [
        (1, "from_a@example.com", EARLIER, "run_a.json"),
        (1, "from_b@example.com", EARLIER, "run_b.json"),
    ])

    assert dedupe(df, "customers").first()["email"] == "from_b@example.com"


def test_missing_timestamp_loses(spark: SparkSession) -> None:
    """A version with no updated_at never beats one that has it."""
    df = _customers(spark, [
        (1, "no_ts@example.com", None, "run_b.json"),
        (1, "has_ts@example.com", EARLIER, "run_a.json"),
    ])

    assert dedupe(df, "customers").first()["email"] == "has_ts@example.com"


def test_unknown_table_fails_loudly(spark: SparkSession) -> None:
    """Dedupe needs a primary key; an unknown table has none."""
    df = _customers(spark, [(1, "a@example.com", EARLIER, "run_a.json")])

    with pytest.raises(ValueError, match="Unknown source table"):
        dedupe(df, "not_a_table")