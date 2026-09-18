"""
Tests for transformation/spark_session.py.

Each config matters for what it does to results, so most tests check the
behaviour rather than reading the config value back.
"""

from pathlib import Path

import pytest
from pyspark.errors import ArithmeticException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformation.spark_session import REQUIRED_CONFIGS

# 2026-08-26T23:30:00Z: still the 26th in UTC, already the 27th in London (BST)
LATE_EVENING_UTC_EPOCH = 1787787000


def test_required_configs_are_applied(spark: SparkSession) -> None:
    """Every REQUIRED_CONFIGS entry is set on the local session."""
    for key, value in REQUIRED_CONFIGS.items():
        assert spark.conf.get(key) == value


def test_timestamp_maps_to_utc_calendar_day(spark: SparkSession) -> None:
    """A timestamp near midnight lands on its UTC day, not the laptop's local day."""
    row = spark.range(1).select(
        F.to_date(F.timestamp_seconds(F.lit(LATE_EVENING_UTC_EPOCH))).alias("day")
    ).first()

    assert str(row["day"]) == "2026-08-26"


def test_division_by_zero_fails_loudly(spark: SparkSession) -> None:
    """ANSI mode turns divide-by-zero in our own logic into an error, not a NULL."""
    with pytest.raises(ArithmeticException):
        spark.range(1).select(F.lit(1) / F.lit(0)).collect()


def test_try_cast_returns_null_for_garbage(spark: SparkSession) -> None:
    """try_cast fails softly even with ANSI on, so bad data can reach dead-letter."""
    row = spark.createDataFrame([("abc",)], ["raw"]).select(
        F.col("raw").try_cast("bigint").alias("parsed")
    ).first()

    assert row["parsed"] is None


def test_delta_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    """The session writes and reads Delta, which MERGE depends on in step 6."""
    path = str(tmp_path / "delta_check")
    spark.range(3).write.format("delta").save(path)

    assert spark.read.format("delta").load(path).count() == 3