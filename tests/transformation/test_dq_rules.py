

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField

from transformation.dq_rules import DQ_ERRORS_COLUMN, apply_dq_rules, split_valid_rejected
from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN
from transformation.schemas.source_schemas import SOURCE_SCHEMAS, to_read_schema
from transformation.type_casting import cast_to_target


def _checked(spark: SparkSession, table: str, rows: list[dict]) -> DataFrame:
    """Build raw string rows for a table, cast them, and apply the DQ rules."""
    schema = (
        to_read_schema(SOURCE_SCHEMAS[table])
        .add(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
        .add(StructField(SOURCE_FILE_COLUMN, StringType(), True))
    )
    typed = cast_to_target(spark.createDataFrame(rows, schema=schema), table)
    return apply_dq_rules(typed, table)


def _reasons(spark: SparkSession, table: str, row: dict) -> list[str]:
    """The DQ reason codes for a single row."""
    return _checked(spark, table, [row]).first()[DQ_ERRORS_COLUMN]


def test_clean_row_has_no_reasons(spark: SparkSession) -> None:
    """A row that passes every rule gets an empty list."""
    assert _reasons(spark, "payments", {"payment_id": "1", "payment_status": "CAPTURED"}) == []


def test_unknown_status_is_rejected(spark: SparkSession) -> None:
    """A value outside the whitelist is quarantined, not passed through."""
    reasons = _reasons(spark, "payments", {"payment_id": "1", "payment_status": "SETTLED_UNKNOWN"})

    assert reasons == ["invalid_payment_status"]


def test_missing_status_is_rejected(spark: SparkSession) -> None:
    """NULL is not an allowed value: isin() alone would have let it through."""
    assert _reasons(spark, "payments", {"payment_id": "1"}) == ["invalid_payment_status"]


def test_zero_quantity_is_rejected(spark: SparkSession) -> None:
    """Zero quantity is an error in the source system."""
    row = {"order_item_id": "1", "product_id": "5", "unit_price": "9.99", "quantity": "0"}

    assert _reasons(spark, "order_items", row) == ["zero_quantity"]


def test_negative_quantity_is_a_return_not_an_error(spark: SparkSession) -> None:
    """Negative quantity means a return. It must reach curated, not dead-letter."""
    row = {"order_item_id": "1", "product_id": "5", "unit_price": "9.99", "quantity": "-1"}

    assert _reasons(spark, "order_items", row) == []


def test_every_broken_rule_is_recorded(spark: SparkSession) -> None:
    """A row breaking two rules shows both, so dead-letter tells the whole story."""
    row = {"order_item_id": "1", "unit_price": "9.99", "quantity": "0"}  # no product_id

    assert _reasons(spark, "order_items", row) == ["null_product_id", "zero_quantity"]


def test_cast_failure_becomes_a_reason(spark: SparkSession) -> None:
    """An unconvertible primary key is both a type failure and a NULL key."""
    reasons = _reasons(spark, "customers", {"customer_id": "abc"})

    assert reasons == ["invalid_type_customer_id", "null_customer_id"]


def test_malformed_line_has_only_the_malformed_reason(spark: SparkSession) -> None:
    """Every column is NULL on a malformed line, so other reasons would be noise."""
    assert _reasons(spark, "order_items", {CORRUPT_RECORD_COLUMN: "{bad"}) == ["malformed_json"]


def test_split_puts_every_row_on_exactly_one_side(spark: SparkSession) -> None:
    """No row is lost and none is counted twice."""
    checked = _checked(spark, "payments", [
        {"payment_id": "1", "payment_status": "CAPTURED"},
        {"payment_id": "2", "payment_status": "SETTLED_UNKNOWN"},
        {"payment_id": "3", "payment_status": "FAILED"},
    ])

    valid, rejected = split_valid_rejected(checked)

    assert valid.count() == 2
    assert rejected.count() == 1
    assert valid.count() + rejected.count() == checked.count()


def test_unknown_table_fails_loudly(spark: SparkSession) -> None:
    """A typo in a table name is an error, not a table with no rules."""
    typed = _checked(spark, "payments", [{"payment_id": "1", "payment_status": "CAPTURED"}])

    with pytest.raises(ValueError, match="Unknown source table"):
        apply_dq_rules(typed, "not_a_table")