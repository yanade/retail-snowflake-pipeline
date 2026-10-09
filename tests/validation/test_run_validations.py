"""Unit tests for validation/run_validations.py: window parsing, source query rendering, the check list and DVT commands."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from transformation.config.table_config import TABLE_CONFIGS
from transformation.schemas.source_schemas import SOURCE_SCHEMAS
from validation.run_validations import (
    COUNT_TABLES,
    RECONCILIATIONS,
    SQL_DIR,
    WINDOW_END_PLACEHOLDER,
    count_command,
    parse_window_end,
    reconciliation_command,
    render_source_query,
    sql_timestamp,
)

WINDOW_END = datetime(2026, 10, 3, 15, 25, 24, tzinfo=timezone.utc)
RUN_ID = "run-1"


# parse_window_end

def test_naive_window_end_is_utc():
    """The watermark table stores naive UTC, so no zone means UTC."""
    assert parse_window_end("2026-10-03 15:25:24") == WINDOW_END


def test_zoned_window_end_is_converted_to_utc():
    assert parse_window_end("2026-10-03 16:25:24+01:00") == WINDOW_END


def test_malformed_window_end_fails_with_the_value():
    with pytest.raises(ValueError, match="2026-10-3"):
        parse_window_end("2026-10-3")


# sql_timestamp and render_source_query

def test_literal_is_quoted_utc_with_microseconds():
    assert sql_timestamp(WINDOW_END) == "'2026-10-03 15:25:24.000000+00'"


def test_render_replaces_the_placeholder(tmp_path: Path):
    query = tmp_path / "x_source.sql"
    query.write_text(f"select 1 from t where created_at <= {WINDOW_END_PLACEHOLDER}")

    assert render_source_query(query, WINDOW_END) == (
        "select 1 from t where created_at <= '2026-10-03 15:25:24.000000+00'"
    )


def test_render_without_placeholder_fails(tmp_path: Path):
    """An unbounded source query would compare a moving table."""
    query = tmp_path / "x_source.sql"
    query.write_text("select 1 from t")

    with pytest.raises(ValueError, match="x_source.sql"):
        render_source_query(query, WINDOW_END)


@pytest.mark.parametrize("path", sorted(SQL_DIR.glob("*_source.sql")), ids=lambda p: p.name)
def test_every_source_query_is_bounded_once(path: Path):
    """Guard on the real files: exactly one placeholder, so the whole source is windowed."""
    assert path.read_text().count(WINDOW_END_PLACEHOLDER) == 1



# The check list

def test_every_source_table_is_validated_once():
    """A new source table must get a check; none may be checked twice."""
    checked = [*COUNT_TABLES, *(check.table for check in RECONCILIATIONS)]
    assert sorted(checked) == sorted(SOURCE_SCHEMAS)


def test_tables_with_reject_rules_are_reconciled():
    """A plain count fails by design where rows go to dead-letter."""
    with_rules = {
        table for table, config in TABLE_CONFIGS.items()
        if config.not_null or config.non_zero or config.allowed_values
    }
    assert with_rules <= {check.table for check in RECONCILIATIONS}


@pytest.mark.parametrize("check", RECONCILIATIONS, ids=lambda c: c.table)
def test_every_reconciliation_has_both_queries(check):
    assert (SQL_DIR / f"{check.table}_source.sql").exists()
    assert (SQL_DIR / f"{check.table}_target.sql").exists()


# Commands

def test_count_command_pairs_every_table_with_its_staging_model():
    command = count_command(WINDOW_END, RUN_ID)
    tables = command[command.index("-tbls") + 1].split(",")

    assert "retail_oltp.customers=DBT_DEV_STAGING.STG_CUSTOMERS" in tables
    assert len(tables) == len(COUNT_TABLES)


def test_count_command_windows_the_source_only():
    command = count_command(WINDOW_END, RUN_ID)

    assert command[command.index("--filters") + 1] == "created_at <= '2026-10-03 15:25:24.000000+00':1=1"


def test_reconciliation_sends_the_rendered_source_query():
    """The window is in the source text; no placeholder may reach DVT."""
    command = reconciliation_command(RECONCILIATIONS[0], WINDOW_END, RUN_ID)
    source_query = command[command.index("-sq") + 1]

    assert "'2026-10-03 15:25:24.000000+00'" in source_query
    assert WINDOW_END_PLACEHOLDER not in source_query


def test_grouping_only_where_declared():
    by_table = {check.table: reconciliation_command(check, WINDOW_END, RUN_ID) for check in RECONCILIATIONS}

    assert "--grouped-columns" not in by_table["order_items"]
    assert by_table["payments"][-2:] == ["--grouped-columns", "currency_code"]


def test_every_command_carries_the_run_id_and_asks_for_json():
    commands = [count_command(WINDOW_END, RUN_ID)] + [
        reconciliation_command(check, WINDOW_END, RUN_ID) for check in RECONCILIATIONS
    ]
    for command in commands:
        assert command[command.index("--run-id") + 1] == RUN_ID
        assert command[command.index("-fmt") + 1] == "json"
