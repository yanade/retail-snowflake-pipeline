"""
Tests for transformation/pipeline.py: the full chain on one batch, from raw
rows to the two Delta outputs.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from transformation.curated_writer import curated_path
from transformation.dead_letter import dead_letter_path
from transformation.pipeline import process_raw_batch
from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN
from transformation.schemas.source_schemas import get_source_schema, to_read_schema


def _raw_order_items(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """read_raw()-shaped order_items rows: every column a string."""
    schema = StructType(
        list(to_read_schema(get_source_schema("order_items")).fields)
        + [
            StructField(CORRUPT_RECORD_COLUMN, StringType(), True),
            StructField(SOURCE_FILE_COLUMN, StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_valid_and_rejected_rows_reach_their_tables(
    spark: SparkSession, tmp_path: Path
) -> None:
    """One good row, one zero-quantity row: each ends up in exactly one output."""
    raw = _raw_order_items(spark, [
        {"order_item_id": "1", "product_id": "5", "quantity": "2", "unit_price": "9.99",
         "updated_at": "2025-06-01T10:00:00Z", SOURCE_FILE_COLUMN: "run_a.json"},
        {"order_item_id": "2", "product_id": "5", "quantity": "0", "unit_price": "9.99",
         "updated_at": "2025-06-01T10:00:00Z", SOURCE_FILE_COLUMN: "run_a.json"},
    ])

    counts = process_raw_batch(spark, raw, "order_items", str(tmp_path))

    assert counts == {"rows_read": 2, "rows_valid": 1, "rows_rejected": 1}
    assert spark.read.format("delta").load(curated_path(str(tmp_path), "order_items")).count() == 1
    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 1


def test_rerunning_the_same_batch_is_a_no_op(spark: SparkSession, tmp_path: Path) -> None:
    """The whole chain is idempotent, not just each writer on its own."""
    raw = _raw_order_items(spark, [
        {"order_item_id": "1", "product_id": "5", "quantity": "2", "unit_price": "9.99",
         "updated_at": "2025-06-01T10:00:00Z", SOURCE_FILE_COLUMN: "run_a.json"},
        {"order_item_id": "2", "product_id": "5", "quantity": "0", "unit_price": "9.99",
         "updated_at": "2025-06-01T10:00:00Z", SOURCE_FILE_COLUMN: "run_a.json"},
    ])

    process_raw_batch(spark, raw, "order_items", str(tmp_path))
    process_raw_batch(spark, raw, "order_items", str(tmp_path))

    assert spark.read.format("delta").load(curated_path(str(tmp_path), "order_items")).count() == 1
    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 1