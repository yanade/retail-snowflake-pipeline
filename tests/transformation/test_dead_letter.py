"""
Tests for transformation/dead_letter.py, on rejected rows shaped like
split_valid_rejected() output.
"""

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from transformation.dead_letter import (
    build_dead_letter_rows,
    dead_letter_path,
    write_dead_letter,
)

REJECTED_SCHEMA = (
    "order_item_id long, updated_at timestamp, _dq_errors array<string>, "
    "_raw_payload string, _corrupt_record string"
)
UPDATED = datetime(2025, 6, 1, 9, 0)
UPDATED_LATER = datetime(2025, 6, 10, 9, 0)
DEAD_LETTER_COLUMNS = [
    "record_id", "source_table", "source_key", "error_reason",
    "raw_payload", "failed_at", "reprocessed", "reprocessed_at",
]


def _rejected_rows(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Rejected order_items as (order_item_id, updated_at, _dq_errors, _raw_payload, _corrupt_record)."""
    return spark.createDataFrame(rows, REJECTED_SCHEMA)


def _rejected(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Rejected parsed order_items as (order_item_id, _dq_errors, _raw_payload), one version each."""
    return _rejected_rows(spark, [(key, UPDATED, errors, payload, None) for key, errors, payload in rows])


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


def test_first_write_records_a_duplicated_reject_once(spark: SparkSession, tmp_path: Path) -> None:
    """The same bad row in two raw files of one batch creates one record."""
    duplicated = (1, ["zero_quantity"], '{"order_item_id":"1"}')
    rejected = _rejected(spark, [duplicated, duplicated])  # e.g. two overlapping ADF runs

    write_dead_letter(spark, rejected, "order_items", str(tmp_path))

    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 1


def test_merge_records_a_duplicated_reject_once(spark: SparkSession, tmp_path: Path) -> None:
    """Insert-only MERGE dedupes against the target, not within the source."""
    existing = _rejected(spark, [(2, ["null_product_id"], '{"order_item_id":"2"}')])
    write_dead_letter(spark, existing, "order_items", str(tmp_path))  # table exists, so the next write MERGEs

    duplicated = (1, ["zero_quantity"], '{"order_item_id":"1"}')
    write_dead_letter(spark, _rejected(spark, [duplicated, duplicated]), "order_items", str(tmp_path))

    assert spark.read.format("delta").load(dead_letter_path(str(tmp_path))).count() == 2


def test_payload_text_does_not_change_the_record_id(spark: SparkSession) -> None:
    """Spark may render the same number token as 0.00 or 0.0; it is still one problem."""
    rows = build_dead_letter_rows(_rejected_rows(spark, [
        (1, UPDATED, ["zero_quantity"], '{"order_item_id":"1","tax_amount":"0.00"}', None),
        (1, UPDATED, ["zero_quantity"], '{"order_item_id":"1","tax_amount":"0.0"}', None),
    ]), "order_items")

    assert rows.select("record_id").distinct().count() == 1


def test_a_new_version_of_the_row_is_a_new_record(spark: SparkSession) -> None:
    """A later updated_at means the source changed the row, so it is a new problem."""
    payload = '{"order_item_id":"1","quantity":"0"}'
    rows = build_dead_letter_rows(_rejected_rows(spark, [
        (1, UPDATED, ["zero_quantity"], payload, None),
        (1, UPDATED_LATER, ["zero_quantity"], payload, None),
    ]), "order_items")

    assert rows.select("record_id").distinct().count() == 2


def test_malformed_lines_are_told_apart_by_their_text(spark: SparkSession) -> None:
    """With no key and no updated_at, the exact line is what identifies the problem."""
    rows = build_dead_letter_rows(_rejected_rows(spark, [
        (None, None, ["malformed_json"], '{"order_item_id": 3, "quan', '{"order_item_id": 3, "quan'),
        (None, None, ["malformed_json"], '{"order_item_id": 4, "pri', '{"order_item_id": 4, "pri'),
    ]), "order_items")

    assert rows.select("record_id").distinct().count() == 2