"""
Tests for transformation/dead_letter.py, on rejected rows shaped like
split_valid_rejected() output.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from transformation.dead_letter import (
    build_dead_letter_rows,
    dead_letter_path,
    write_dead_letter,
)

REJECTED_SCHEMA = "order_item_id long, _dq_errors array<string>, _raw_payload string"
DEAD_LETTER_COLUMNS = [
    "record_id", "source_table", "source_key", "error_reason",
    "raw_payload", "failed_at", "reprocessed", "reprocessed_at",
]


def _rejected(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Rejected order_items as (order_item_id, _dq_errors, _raw_payload)."""
    return spark.createDataFrame(rows, REJECTED_SCHEMA)


def test_shape_matches_the_dead_letter_schema(spark: SparkSession) -> None:
    """The columns are exactly those declared in CLAUDE.md, in order."""
    rejected = _rejected(spark, [(1, ["zero_quantity"], '{"order_item_id":"1"}')])

    assert build_dead_letter_rows(rejected, "order_items").columns == DEAD_LETTER_COLUMNS


def test_reasons_and_key_are_recorded(spark: SparkSession) -> None:
    """All reasons are kept in one VARCHAR, and source_key is the primary key."""
    rejected = _rejected(spark, [(7, ["null_product_id", "zero_quantity"], "{}")])

    row = build_dead_letter_rows(rejected, "order_items").first()

    assert row["source_table"] == "order_items"
    assert row["source_key"] == "7"
    assert row["error_reason"] == "null_product_id;zero_quantity"
    assert row["reprocessed"] is False
    assert row["reprocessed_at"] is None


def test_same_problem_gets_the_same_record_id(spark: SparkSession) -> None:
    """The hash excludes failed_at, so the same bad row is the same record."""
    payload = '{"order_item_id":"1","quantity":"0"}'
    first = build_dead_letter_rows(_rejected(spark, [(1, ["zero_quantity"], payload)]), "order_items")
    again = build_dead_letter_rows(_rejected(spark, [(1, ["zero_quantity"], payload)]), "order_items")

    assert first.first()["record_id"] == again.first()["record_id"]


def test_a_different_reason_is_a_different_record(spark: SparkSession) -> None:
    """The same row failing for a new reason is a new problem, not a duplicate."""
    payload = '{"order_item_id":"1"}'
    zero = build_dead_letter_rows(_rejected(spark, [(1, ["zero_quantity"], payload)]), "order_items")
    null_product = build_dead_letter_rows(_rejected(spark, [(1, ["null_product_id"], payload)]), "order_items")

    assert zero.first()["record_id"] != null_product.first()["record_id"]


def test_first_write_creates_the_table(spark: SparkSession, tmp_path: Path) -> None:
    """Rejected rows land in the shared dead-letter table."""
    rejected = _rejected(spark, [
        (1, ["zero_quantity"], '{"order_item_id":"1"}'),
        (2, ["null_product_id"], '{"order_item_id":"2"}'),
    ])

    path = write_dead_letter(spark, rejected, "order_items", str(tmp_path))

    assert spark.read.format("delta").load(path).count() == 2
    assert path == dead_letter_path(str(tmp_path))


def test_rewriting_the_same_rejects_inserts_nothing(spark: SparkSession, tmp_path: Path) -> None:
    """ADF re-copies rejected rows every run; dead-letter must not grow."""
    rejected = _rejected(spark, [(1, ["zero_quantity"], '{"order_item_id":"1"}')])

    write_dead_letter(spark, rejected, "order_items", str(tmp_path))
    write_dead_letter(spark, rejected, "order_items", str(tmp_path))

    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 1