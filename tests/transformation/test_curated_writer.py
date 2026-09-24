"""
Tests for transformation/curated_writer.py, writing real Delta tables into
pytest's tmp_path. Each test starts from an empty folder.
"""

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from transformation.curated_writer import curated_path, merge_into_curated
from transformation.dq_rules import DQ_ERRORS_COLUMN
from transformation.raw_reader import SOURCE_FILE_COLUMN
from transformation.schemas.source_schemas import get_source_schema

EARLIER = datetime(2025, 6, 1, 9, 0)
LATER = datetime(2025, 6, 10, 9, 0)


def _valid_customers(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Deduplicated customer rows, still carrying pipeline metadata."""
    schema = StructType(
        list(get_source_schema("customers").fields)  # copy: add() mutates in place
        + [
            StructField(SOURCE_FILE_COLUMN, StringType(), True),
            StructField(DQ_ERRORS_COLUMN, ArrayType(StringType()), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_first_run_creates_the_table_with_source_columns_only(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Curated mirrors the source shape: no _raw_payload, no _dq_errors."""
    df = _valid_customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
    ])

    path = merge_into_curated(spark, df, "customers", str(tmp_path))
    written = spark.read.format("delta").load(path)

    assert written.columns == get_source_schema("customers").fieldNames()
    assert written.count() == 1


def test_newer_version_updates_the_row(spark: SparkSession, tmp_path: Path) -> None:
    """A later updated_at replaces the stored values."""
    first = _valid_customers(spark, [
        {"customer_id": 1, "email": "old@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
    ])
    second = _valid_customers(spark, [
        {"customer_id": 1, "email": "new@example.com", "updated_at": LATER,
         SOURCE_FILE_COLUMN: "run_b.json", DQ_ERRORS_COLUMN: []},
    ])

    merge_into_curated(spark, first, "customers", str(tmp_path))
    merge_into_curated(spark, second, "customers", str(tmp_path))

    stored = spark.read.format("delta").load(curated_path(str(tmp_path), "customers"))
    assert stored.count() == 1
    assert stored.first()["email"] == "new@example.com"


def test_older_version_does_not_overwrite(spark: SparkSession, tmp_path: Path) -> None:
    """The guard: re-reading older data cannot move curated backwards."""
    current = _valid_customers(spark, [
        {"customer_id": 1, "email": "current@example.com", "updated_at": LATER,
         SOURCE_FILE_COLUMN: "run_b.json", DQ_ERRORS_COLUMN: []},
    ])
    stale = _valid_customers(spark, [
        {"customer_id": 1, "email": "stale@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
    ])

    merge_into_curated(spark, current, "customers", str(tmp_path))
    merge_into_curated(spark, stale, "customers", str(tmp_path))

    stored = spark.read.format("delta").load(curated_path(str(tmp_path), "customers"))
    assert stored.first()["email"] == "current@example.com"


def test_new_key_is_inserted(spark: SparkSession, tmp_path: Path) -> None:
    """A key that isn't in curated yet is added, not ignored."""
    first = _valid_customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
    ])
    second = _valid_customers(spark, [
        {"customer_id": 2, "email": "b@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_b.json", DQ_ERRORS_COLUMN: []},
    ])

    merge_into_curated(spark, first, "customers", str(tmp_path))
    merge_into_curated(spark, second, "customers", str(tmp_path))

    stored = spark.read.format("delta").load(curated_path(str(tmp_path), "customers"))
    assert stored.count() == 2


def test_rerunning_the_same_batch_changes_nothing(spark: SparkSession, tmp_path: Path) -> None:
    """Idempotency: the same input twice leaves curated identical."""
    df = _valid_customers(spark, [
        {"customer_id": 1, "email": "a@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
        {"customer_id": 2, "email": "b@example.com", "updated_at": EARLIER,
         SOURCE_FILE_COLUMN: "run_a.json", DQ_ERRORS_COLUMN: []},
    ])

    merge_into_curated(spark, df, "customers", str(tmp_path))
    merge_into_curated(spark, df, "customers", str(tmp_path))

    stored = spark.read.format("delta").load(curated_path(str(tmp_path), "customers"))
    assert stored.count() == 2