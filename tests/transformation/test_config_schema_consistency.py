
import pytest

from generate_data import ORDER_STATUSES, PAYMENT_STATUSES
from transformation.config.table_config import TABLE_CONFIGS
from transformation.schemas.source_schemas import SOURCE_SCHEMAS


INJECTED_BAD_PAYMENT_STATUS = "SETTLED_UNKNOWN"


# ── coverage ───
def test_config_and_schemas_cover_the_same_tables():
    """Every configured table has a schema, and every schema has a config."""
    assert set(TABLE_CONFIGS) == set(SOURCE_SCHEMAS)


# ── per-table checks ───

@pytest.mark.parametrize("table_name", sorted(TABLE_CONFIGS))
def test_registry_key_matches_source_table(table_name):
    """The dict key and the object's own source_table cannot drift apart."""
    assert TABLE_CONFIGS[table_name].source_table == table_name


@pytest.mark.parametrize("table_name", sorted(TABLE_CONFIGS))
def test_primary_key_is_declared(table_name):
    """A MERGE with no key would silently degrade into an append."""
    assert TABLE_CONFIGS[table_name].primary_key


@pytest.mark.parametrize("table_name", sorted(TABLE_CONFIGS))
def test_configured_columns_exist_in_the_schema(table_name):
    """
    Every column named in a config exists in that table's schema.

    Catches typos such as a stray space or bracket inside a column name. Those
    are valid Python and would otherwise surface as a rule that never fires.
    """
    config = TABLE_CONFIGS[table_name]
    schema_columns = set(SOURCE_SCHEMAS[table_name].fieldNames())

    named = set(config.primary_key)
    named |= set(config.not_null)
    named |= set(config.non_zero)
    named |= set(config.allowed_values)
    named.add(config.watermark_column)

    unknown = named - schema_columns
    assert not unknown, f"{table_name}: not in schema: {sorted(unknown)}"


# ── whitelists against the source system ───

def test_order_status_whitelist_matches_the_source():
    
    whitelist = set(TABLE_CONFIGS["orders"].allowed_values["order_status"])

    assert whitelist == set(ORDER_STATUSES)


def test_payment_status_whitelist_excludes_only_the_injected_value():
    
    whitelist = set(TABLE_CONFIGS["payments"].allowed_values["payment_status"])

    assert set(PAYMENT_STATUSES) - whitelist == {INJECTED_BAD_PAYMENT_STATUS}