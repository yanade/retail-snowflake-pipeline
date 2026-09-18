"""
Tests for transformation/type_casting.py, on hand-built string DataFrames
shaped like read_raw() output.
"""

from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField

from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN
from transformation.schemas.source_schemas import SOURCE_SCHEMAS, to_read_schema
from transformation.type_casting import CAST_ERRORS_COLUMN, cast_to_target


def _raw_payments(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Build a read_raw()-shaped payments DataFrame. Keys left out become NULL."""
    schema = (
        to_read_schema(SOURCE_SCHEMAS["payments"])
        .add(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
        .add(StructField(SOURCE_FILE_COLUMN, StringType(), True))
    )
    return spark.createDataFrame(rows, schema=schema)


def test_valid_strings_convert_to_target_types(spark: SparkSession) -> None:
    """Numbers, decimals and ISO timestamps convert, with no errors recorded."""
    raw = _raw_payments(spark, [
        {"payment_id": "7", "payment_amount": "19.99", "payment_date": "2025-01-02T03:04:05Z"},
    ])

    row = cast_to_target(raw, "payments").select(
        "payment_id",
        "payment_amount",
        # formatted by Spark in the session zone (UTC), not converted by Python
        F.date_format("payment_date", "yyyy-MM-dd HH:mm:ss").alias("payment_date"),
        CAST_ERRORS_COLUMN,
    ).first()

    assert row["payment_id"] == 7
    assert row["payment_amount"] == Decimal("19.99")
    assert row["payment_date"] == "2025-01-02 03:04:05"
    assert row[CAST_ERRORS_COLUMN] == []


def test_unconvertible_values_are_null_and_named(spark: SparkSession) -> None:
    """Garbage becomes NULL, and _cast_errors says which columns, in schema order."""
    raw = _raw_payments(spark, [{"payment_id": "abc", "payment_amount": "12,50"}])

    row = cast_to_target(raw, "payments").first()

    assert row["payment_id"] is None
    assert row["payment_amount"] is None
    assert row[CAST_ERRORS_COLUMN] == ["payment_id", "payment_amount"]


def test_missing_value_is_not_a_cast_error(spark: SparkSession) -> None:
    """A NULL in the raw data is missing, not failed. DQ decides if that's allowed."""
    raw = _raw_payments(spark, [{"payment_id": "7"}])  # every other column NULL

    row = cast_to_target(raw, "payments").first()

    assert row[CAST_ERRORS_COLUMN] == []


def test_decimal_overflow_is_flagged(spark: SparkSession) -> None:
    """A number too large for DECIMAL(12,2) is a failed conversion, not a rounding."""
    raw = _raw_payments(spark, [{"payment_amount": "12345678901234.00"}])  # 14 digits before the point, max is 10

    row = cast_to_target(raw, "payments").first()

    assert row["payment_amount"] is None
    assert row[CAST_ERRORS_COLUMN] == ["payment_amount"]


def test_metadata_columns_pass_through(spark: SparkSession) -> None:
    """_corrupt_record and _source_file survive unchanged, before _cast_errors."""
    raw = _raw_payments(spark, [{"payment_id": "7", SOURCE_FILE_COLUMN: "payments_run1.json"}])

    out = cast_to_target(raw, "payments")

    assert out.columns[-3:] == [CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN, CAST_ERRORS_COLUMN]
    assert out.first()[SOURCE_FILE_COLUMN] == "payments_run1.json"


def test_unknown_table_fails_loudly(spark: SparkSession) -> None:
    """A typo in a table name is an error, not a silently wrong schema."""
    raw = _raw_payments(spark, [{"payment_id": "7"}])

    with pytest.raises(ValueError, match="Unknown source table"):
        cast_to_target(raw, "not_a_table")