"""
Tests for transformation/raw_payload.py, on hand-built DataFrames shaped like
read_raw() output. No files needed: the function only sees columns.
"""

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField

from transformation.raw_payload import RAW_PAYLOAD_COLUMN, add_raw_payload
from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN
from transformation.schemas.source_schemas import SOURCE_SCHEMAS, to_read_schema
from transformation.type_casting import CAST_ERRORS_COLUMN, cast_to_target

MALFORMED_LINE = '{"customer_id": 3, "email": "c@example.com"'  # no closing brace


def _raw_customers(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Build a read_raw()-shaped customers DataFrame. Keys left out become NULL."""
    schema = (
        to_read_schema(SOURCE_SCHEMAS["customers"])
        .add(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
        .add(StructField(SOURCE_FILE_COLUMN, StringType(), True))
    )
    return spark.createDataFrame(rows, schema=schema)


def test_payload_contains_the_original_values(spark: SparkSession) -> None:
    """A clean row's payload holds its values as they were read."""
    raw = _raw_customers(spark, [{"customer_id": "1", "email": "a@example.com"}])

    payload = json.loads(add_raw_payload(raw).first()[RAW_PAYLOAD_COLUMN])

    assert payload["customer_id"] == "1"
    assert payload["email"] == "a@example.com"


def test_payload_keeps_explicit_nulls(spark: SparkSession) -> None:
    """A NULL column appears as null in the payload, not as a missing key."""
    raw = _raw_customers(spark, [{"customer_id": "abc"}])  # email and the rest are NULL

    payload = json.loads(add_raw_payload(raw).first()[RAW_PAYLOAD_COLUMN])

    assert payload["customer_id"] == "abc"
    assert "email" in payload
    assert payload["email"] is None


def test_payload_excludes_reader_metadata(spark: SparkSession) -> None:
    """_corrupt_record and _source_file describe the read, not the record."""
    raw = _raw_customers(spark, [{"customer_id": "1", SOURCE_FILE_COLUMN: "customers_run1.json"}])

    payload = json.loads(add_raw_payload(raw).first()[RAW_PAYLOAD_COLUMN])

    assert CORRUPT_RECORD_COLUMN not in payload
    assert SOURCE_FILE_COLUMN not in payload


def test_payload_of_malformed_line_is_the_original_text(spark: SparkSession) -> None:
    """For a line that wasn't valid JSON, the payload is the line itself."""
    raw = _raw_customers(spark, [{CORRUPT_RECORD_COLUMN: MALFORMED_LINE}])

    row = add_raw_payload(raw).select("customer_id", RAW_PAYLOAD_COLUMN).first()

    assert row[RAW_PAYLOAD_COLUMN] == MALFORMED_LINE


def test_payload_survives_casting(spark: SparkSession) -> None:
    """After casting, the value is NULL but the payload still shows what arrived."""
    raw = _raw_customers(spark, [{"customer_id": "abc"}])

    row = cast_to_target(add_raw_payload(raw), "customers").first()

    assert row["customer_id"] is None
    assert row[CAST_ERRORS_COLUMN] == ["customer_id"]
    assert json.loads(row[RAW_PAYLOAD_COLUMN])["customer_id"] == "abc"