"""
Tests for transformation/pipeline.py: the full chain on one batch, from raw
rows to the two Delta outputs.
"""
import pytest
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from transformation.curated_writer import curated_path
from transformation.dead_letter import dead_letter_path
from transformation.pipeline import process_raw_batch
from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN, read_raw
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


BOM = "\ufeff"  # what ADF writes into the file of a copy that moved 0 rows


@pytest.mark.parametrize("artifact", [BOM, BOM + " "])
def test_an_empty_export_artifact_is_dropped(
    spark: SparkSession, tmp_path: Path, artifact: str
) -> None:
    """A BOM-only record is not data: it is not read, rejected or dead-lettered."""
    raw = _raw_order_items(spark, [
        {"order_item_id": "1", "product_id": "5", "quantity": "2", "unit_price": "9.99",
         "updated_at": "2025-06-01T10:00:00Z", SOURCE_FILE_COLUMN: "run_a.json"},
        {CORRUPT_RECORD_COLUMN: artifact, SOURCE_FILE_COLUMN: "empty_run.json"},
    ])

    counts = process_raw_batch(spark, raw, "order_items", str(tmp_path))

    assert counts == {"rows_read": 1, "rows_valid": 1, "rows_rejected": 0}
    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 0


def test_a_genuinely_malformed_line_is_still_dead_lettered(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The artifact filter must not swallow lines that have real content."""
    raw = _raw_order_items(spark, [
        {CORRUPT_RECORD_COLUMN: BOM + '{"order_item_id": 3, "quan', SOURCE_FILE_COLUMN: "run_a.json"},
    ])

    counts = process_raw_batch(spark, raw, "order_items", str(tmp_path))

    assert counts == {"rows_read": 1, "rows_valid": 0, "rows_rejected": 1}


def test_artifacts_are_dropped_when_reading_real_files(spark: SparkSession, tmp_path: Path) -> None:
    """Through read_raw(), as on Databricks: Spark restricts queries on _corrupt_record alone."""
    folder = tmp_path / "raw" / "order_items" / "year=2026" / "month=10" / "day=02"
    folder.mkdir(parents=True)
    (folder / "run_a.json").write_text(
        '{"order_item_id":1,"product_id":5,"quantity":2,"unit_price":9.99,'
        '"updated_at":"2025-06-01T10:00:00Z"}\n'
    )
    (folder / "empty_run.json").write_bytes(b"\xef\xbb\xbf")  # what ADF writes for a 0-row copy

    raw = read_raw(spark, "order_items", str(tmp_path / "raw"))
    counts = process_raw_batch(spark, raw, "order_items", str(tmp_path / "curated"))

    assert counts == {"rows_read": 1, "rows_valid": 1, "rows_rejected": 0}