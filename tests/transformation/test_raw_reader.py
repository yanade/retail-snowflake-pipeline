"""
Tests for transformation/raw_reader.py, on JSON Lines files laid out the way
ADF writes them: <table>/year=YYYY/month=MM/day=DD/<file>.json
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN, read_raw
from transformation.schemas.source_schemas import SOURCE_SCHEMAS, to_read_schema

RAW_LINES = [
    '{"customer_id": 1, "email": "a@example.com"}',  # valid
    '{"customer_id": "abc", "email": null}',          # valid JSON, garbage id
    '{"customer_id": 3, "email": "c@example.com"',    # malformed: no closing brace
]


@pytest.fixture
def raw_root(tmp_path: Path) -> str:
    """A raw zone holding one customers file, in ADF's partitioned layout."""
    folder = tmp_path / "customers" / "year=2026" / "month=09" / "day=17"
    folder.mkdir(parents=True)
    (folder / "customers_run1.json").write_text("\n".join(RAW_LINES) + "\n")
    return str(tmp_path)


def test_read_schema_is_all_strings() -> None:
    """The read schema keeps every target column, in order, as a string."""
    read_schema = to_read_schema(SOURCE_SCHEMAS["customers"])

    assert read_schema.fieldNames() == SOURCE_SCHEMAS["customers"].fieldNames()
    assert all(isinstance(f.dataType, StringType) for f in read_schema.fields)


def test_unparseable_value_survives_as_text(spark: SparkSession, raw_root: str) -> None:
    """A typed read would turn 'abc' into NULL; a string read keeps it for DQ."""
    ids = {r["customer_id"] for r in read_raw(spark, "customers", raw_root).collect()}

    assert "abc" in ids
    assert "1" in ids


def test_malformed_line_lands_in_corrupt_record(spark: SparkSession, raw_root: str) -> None:
    """A line that isn't valid JSON becomes a row, keeping the original text."""
    rows = (
        read_raw(spark, "customers", raw_root)
        .filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull())
        .select("customer_id", CORRUPT_RECORD_COLUMN)  # Spark refuses a query on the corrupt column alone
        .collect()
    )

    assert len(rows) == 1
    assert rows[0]["customer_id"] is None
    assert rows[0][CORRUPT_RECORD_COLUMN] == RAW_LINES[2]


def test_extraction_partitions_are_dropped(spark: SparkSession, raw_root: str) -> None:
    """year/month/day are extraction dates and must not reach the business logic."""
    columns = set(read_raw(spark, "customers", raw_root).columns)

    assert not {"year", "month", "day"} & columns


def test_source_file_is_recorded(spark: SparkSession, raw_root: str) -> None:
    """Every row carries the raw file it came from, for dead-letter traceability."""
    files = {r[SOURCE_FILE_COLUMN] for r in read_raw(spark, "customers", raw_root).collect()}

    assert len(files) == 1
    assert files.pop().endswith("customers_run1.json")


def test_unknown_table_fails_loudly(spark: SparkSession, raw_root: str) -> None:
    """A typo in a table name is an error, not an empty DataFrame."""
    with pytest.raises(ValueError, match="Unknown source table"):
        read_raw(spark, "not_a_table", raw_root)