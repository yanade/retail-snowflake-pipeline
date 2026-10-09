"""Unit tests for validation/dvt_suite.py: window, check list, DVT commands, results and connections."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from transformation.config.table_config import TABLE_CONFIGS
from transformation.schemas.source_schemas import SOURCE_SCHEMAS
from validation.dvt_suite import (
    COUNT_TABLES,
    RECONCILIATIONS,
    SQL_DIR,
    WINDOW_END_PLACEHOLDER,
    add_connections,
    count_command,
    failed_rows,
    missing_tables,
    parse_dvt_output,
    parse_window_end,
    postgres_connection_args,
    reconciliation_command,
    render_source_query,
    run_dvt,
    snowflake_connection_args,
    sql_timestamp,
)

WINDOW_END = datetime(2026, 10, 3, 15, 25, 24, tzinfo=timezone.utc)
RUN_ID = "run-1"
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": "ab12345", "SNOWFLAKE_USER": "loader", "SNOWFLAKE_ROLE": "RETAIL_DEV",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH", "SNOWFLAKE_DATABASE": "ecommerce_db",
    "SNOWFLAKE_PRIVATE_KEY_PATH": "/keys/rsa_key.p8",
}


def _row(table: str | None, status: str = "success") -> dict:
    """One DVT result row, reduced to the fields the runner reads."""
    return {"source_table_name": table, "validation_name": "count", "validation_status": status}


def _line(*rows: dict) -> str:
    """One stdout line as DVT -fmt json prints it: rows keyed "0", "1", ..."""
    return json.dumps({str(i): row for i, row in enumerate(rows)})


def _fake_dvt(stdout: str, exit_code: int = 0) -> list[str]:
    """A command that prints stdout and exits like DVT would, run through a real subprocess."""
    return [sys.executable, "-c", f"import sys; sys.stdout.write({stdout!r}); sys.exit({exit_code})"]


def _value(args: list[str], flag: str) -> str:
    """The value that follows a flag in an argument list."""
    return args[args.index(flag) + 1]


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



# parse_dvt_output

def test_every_line_is_parsed():
    """A multi-table run prints one JSON object per table; reading only the last line hides the rest."""
    stdout = "\n".join([_line(_row("retail_oltp.currencies")), _line(_row("retail_oltp.stores"))]) + "\n"

    assert [row["source_table_name"] for row in parse_dvt_output(stdout)] == [
        "retail_oltp.currencies", "retail_oltp.stores",
    ]


def test_every_row_of_a_line_is_parsed():
    """A grouped reconciliation prints several rows in one object."""
    assert len(parse_dvt_output(_line(_row(None), _row(None), _row(None)))) == 3


def test_a_line_that_is_not_json_fails():
    with pytest.raises(json.JSONDecodeError):
        parse_dvt_output("Traceback (most recent call last):\n")


# missing_tables and failed_rows

def test_a_table_without_a_result_is_missing():
    rows = [_row("retail_oltp.currencies")]

    assert missing_tables(rows, ("currencies", "stores")) == ["stores"]


def test_fail_and_unknown_statuses_are_failures():
    rows = [_row("a"), _row("b", "fail"), _row("c", "error"), {"source_table_name": "d"}]

    assert [row["source_table_name"] for row in failed_rows(rows)] == ["b", "c", "d"]


# run_dvt

def test_run_dvt_returns_the_printed_rows():
    rows = run_dvt(_fake_dvt(_line(_row("retail_oltp.stores")) + "\n"))

    assert rows == [_row("retail_oltp.stores")]


def test_run_dvt_fails_when_dvt_exits_non_zero():
    """DVT crashed: the check did not run, which is not the same as a mismatch."""
    with pytest.raises(RuntimeError, match="exited 1"):
        run_dvt(_fake_dvt("", exit_code=1))


def test_run_dvt_fails_when_dvt_prints_nothing():
    with pytest.raises(RuntimeError, match="no results"):
        run_dvt(_fake_dvt(""))


# Connections

def test_postgres_password_is_url_decoded():
    """A ! is stored as %21 in a URL; DVT needs the real password."""
    args = postgres_connection_args("postgresql://admin:S3cret%21x@db.example.com:5432/retail_source")

    assert _value(args, "--password") == "S3cret!x"
    assert _value(args, "--host") == "db.example.com"
    assert _value(args, "--database") == "retail_source"


def test_postgres_port_defaults_to_5432():
    args = postgres_connection_args("postgresql://admin:pw@db.example.com/retail_source")

    assert _value(args, "--port") == "5432"


def test_postgres_url_without_password_fails_and_names_it():
    with pytest.raises(ValueError, match="password"):
        postgres_connection_args("postgresql://admin@db.example.com/retail_source")


def test_snowflake_uses_the_key_and_the_staging_schema():
    args = snowflake_connection_args(SNOWFLAKE_ENV)

    assert _value(args, "--database") == "ecommerce_db/DBT_DEV_STAGING"
    assert json.loads(_value(args, "--connect-args")) == {"private_key_file": "/keys/rsa_key.p8"}
    assert _value(args, "--password") == ""


def test_snowflake_missing_variable_is_named():
    env = {k: v for k, v in SNOWFLAKE_ENV.items() if k != "SNOWFLAKE_ROLE"}

    with pytest.raises(ValueError, match="SNOWFLAKE_ROLE"):
        snowflake_connection_args(env)


def test_add_connections_needs_database_url(tmp_path: Path):
    """Fails before any subprocess, so nothing is half-created."""
    with pytest.raises(ValueError, match="DATABASE_URL"):
        add_connections(SNOWFLAKE_ENV, str(tmp_path))
