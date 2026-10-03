"""Guard: create_raw_tables.sql declares the same columns as source_schemas.py."""

import re
from pathlib import Path

import pytest

from transformation.schemas.source_schemas import SOURCE_SCHEMAS

DDL_PATH = Path(__file__).resolve().parents[2] / "snowflake" / "ddl" / "create_raw_tables.sql"
TABLE_PATTERN = re.compile(r"CREATE TABLE IF NOT EXISTS ecommerce_db\.raw\.(\w+) \((.*?)\n\)", re.S)
METADATA_PREFIX = "_"  # load metadata columns, not source columns


def _ddl_columns() -> dict[str, list[str]]:
    """
    Source column names per table, in declaration order.

    Returns:
        Mapping of table name to its column names, metadata columns excluded.
    """
    tables = {}
    for name, body in TABLE_PATTERN.findall(DDL_PATH.read_text()):
        columns = [line.split()[0] for line in body.strip().splitlines() if line.strip()]  # first word is the name
        tables[name] = [c for c in columns if not c.startswith(METADATA_PREFIX)]
    return tables


def test_ddl_has_one_table_per_source_table():
    """A table added to source_schemas.py must also be added to the DDL, and vice versa."""
    assert set(_ddl_columns()) == set(SOURCE_SCHEMAS)


@pytest.mark.parametrize("table", sorted(SOURCE_SCHEMAS))
def test_ddl_columns_match_source_schema(table):
    """Same names in the same order; order matters to a reader, MATCH_BY_COLUMN_NAME ignores it."""
    assert _ddl_columns()[table] == SOURCE_SCHEMAS[table].fieldNames()
