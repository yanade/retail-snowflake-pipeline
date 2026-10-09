"""Guard: create_raw_tables.sql declares the same columns as every served table."""

import re
from pathlib import Path

import pytest

from transformation.dead_letter import DEAD_LETTER_COLUMNS, DEAD_LETTER_TABLE
from transformation.schemas.source_schemas import SOURCE_SCHEMAS

DDL_PATH = Path(__file__).resolve().parents[2] / "snowflake" / "ddl" / "create_raw_tables.sql"
TABLE_PATTERN = re.compile(r"CREATE TABLE IF NOT EXISTS ecommerce_db\.raw\.(\w+) \((.*?)\n\)", re.S)
METADATA_PREFIX = "_"  # load metadata columns, not source columns
EXPECTED_COLUMNS = {
    **{table: schema.fieldNames() for table, schema in SOURCE_SCHEMAS.items()},
    DEAD_LETTER_TABLE: list(DEAD_LETTER_COLUMNS),
}  # every served table: the source tables plus dead_letter

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


def test_ddl_has_one_table_per_served_table():
    """A table added to the served export must also be added to the DDL, and vice versa."""
    assert set(_ddl_columns()) == set(EXPECTED_COLUMNS)


@pytest.mark.parametrize("table", sorted(EXPECTED_COLUMNS))
def test_ddl_columns_match_declared_columns(table):
    """Same names in the same order; order matters to a reader, MATCH_BY_COLUMN_NAME ignores it."""
    assert _ddl_columns()[table] == EXPECTED_COLUMNS[table]
