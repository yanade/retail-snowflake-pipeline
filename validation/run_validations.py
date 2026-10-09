"""Run the DVT suite for one load window and fail loudly on any mismatch (ADR-020)."""
import sys
import json
import subprocess
import os
import tempfile

from dataclasses import dataclass

from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping
from urllib.parse import unquote, urlsplit

SQL_DIR = Path(__file__).resolve().parent / "sql"
WINDOW_END_PLACEHOLDER = "{window_end}"  # replaced by a UTC timestamp literal in every *_source.sql
SOURCE_CONN = "pg_source"
TARGET_CONN = "sf_dev"
SOURCE_SCHEMA = "retail_oltp"
TARGET_SCHEMA = "DBT_DEV_STAGING"  # upper case, or ibis quotes the name and Snowflake cannot find it
SUCCESS_STATUS = "success"  # anything else, including an unknown status, is a failure
COUNT_TABLES = (  # no reject rules, so source and staging must have the same row count
    "currencies", "product_categories", "suppliers", "products", "customers",
    "customer_addresses", "stores", "employees", "exchange_rates",
)
CONN_HOME_VARIABLE = "PSO_DV_CONN_HOME"  # DVT reads and writes connections in this folder
DEFAULT_POSTGRES_PORT = 5432
SNOWFLAKE_VARIABLES = (
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE", "SNOWFLAKE_PRIVATE_KEY_PATH",
)


@dataclass(frozen=True)
class Reconciliation:
    """A table with reject rules: source must equal the newest accepted or rejected version per key."""

    table: str
    key: str
    sums: tuple[str, ...]
    grouped_by: str | None = None


RECONCILIATIONS = (
    Reconciliation("order_items", "order_item_id", ("quantity", "unit_price_cents", "line_amount_cents")),
    Reconciliation("payments", "payment_id", ("payment_amount_cents",), grouped_by="currency_code"),
    Reconciliation("orders", "order_id", ("total_amount_cents",), grouped_by="currency_code"),
)


def _dvt(*args: str) -> list[str]:
    """DVT command line run by this interpreter, so the runner and DVT share venv-dvt."""
    return [sys.executable, "-m", "data_validation", *args]


def count_command(window_end: datetime, run_id: str) -> list[str]:
    """
    One DVT call comparing row counts of every table without reject rules.

    Args:
        window_end: Aware UTC datetime from parse_window_end().
        run_id: Id shared by every validation of this run.

    Returns:
        The command as an argument list.
    """
    tables = ",".join(f"{SOURCE_SCHEMA}.{t}={TARGET_SCHEMA}.STG_{t.upper()}" for t in COUNT_TABLES)
    return _dvt(
        "validate", "column",
        "-sc", SOURCE_CONN, "-tc", TARGET_CONN,
        "-tbls", tables,
        "--filters", f"created_at <= {sql_timestamp(window_end)}:1=1",  # source windowed, target not
        "--run-id", run_id, "-fmt", "json",
    )


def reconciliation_command(check: Reconciliation, window_end: datetime, run_id: str) -> list[str]:
    """
    One DVT call reconciling a table with reject rules against staging plus dead-letter.

    Args:
        check: Which table, key, sums and grouping.
        window_end: Aware UTC datetime from parse_window_end().
        run_id: Id shared by every validation of this run.

    Returns:
        The command as an argument list.
    """
    command = _dvt(
        "validate", "custom-query", "column",
        "-sc", SOURCE_CONN, "-tc", TARGET_CONN,
        "-sq", render_source_query(SQL_DIR / f"{check.table}_source.sql", window_end),  # text: the window is in it
        "-tqf", str(SQL_DIR / f"{check.table}_target.sql"),
        "--count", check.key,
        "--sum", ",".join(check.sums),
        "--run-id", run_id, "-fmt", "json",
    )
    if check.grouped_by:
        command += ["--grouped-columns", check.grouped_by]
    return command


def parse_window_end(value: str) -> datetime:
    """
    Parse the load window end; a value without a zone is UTC, as the watermark table stores it.

    Args:
        value: ISO timestamp, e.g. '2026-10-03 15:25:24'.

    Returns:
        The window end as an aware UTC datetime.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"window_end must be an ISO timestamp like '2026-10-03 15:25:24', got {value!r}") from error
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sql_timestamp(window_end: datetime) -> str:
    """
    Format the window end as a PostgreSQL literal with an explicit UTC offset.

    Args:
        window_end: Aware UTC datetime from parse_window_end().

    Returns:
        A quoted literal, e.g. '2026-10-03 15:25:24.000000+00'.
    """
    return f"'{window_end:%Y-%m-%d %H:%M:%S.%f}+00'"


def render_source_query(path: Path, window_end: datetime) -> str:
    """
    Read a source query and bound it by the load window.

    Args:
        path: A *_source.sql file.
        window_end: Aware UTC datetime from parse_window_end().

    Returns:
        The query text with the placeholder replaced.
    """
    text = path.read_text()
    if WINDOW_END_PLACEHOLDER not in text:
        raise ValueError(f"{path.name} has no {WINDOW_END_PLACEHOLDER}; an unbounded source query compares a moving table")
    return text.replace(WINDOW_END_PLACEHOLDER, sql_timestamp(window_end))



def run_dvt(command: list[str], env: Mapping[str, str] | None = None) -> list[dict]:
    """
    Run one DVT command and return every validation row it printed.

    Args:
        command: Argument list from count_command() or reconciliation_command().
        env: Environment for DVT, with PSO_DV_CONN_HOME; None inherits this process's.

    Returns:
        One dict per validation row.
    """
    completed = subprocess.run(command, capture_output=True, text=True, env=env)  # no shell: arguments arrive exactly as built
    if completed.returncode != 0:
        raise RuntimeError(f"DVT exited {completed.returncode}, the check did not run:\n{completed.stderr[-2000:]}")
    rows = parse_dvt_output(completed.stdout)
    if not rows:
        raise RuntimeError(f"DVT printed no results:\n{completed.stderr[-2000:]}")
    return rows


def parse_dvt_output(stdout: str) -> list[dict]:
    """
    Parse DVT's -fmt json output: one JSON object per line, one line per table.

    Args:
        stdout: Everything DVT wrote to stdout.

    Returns:
        Every validation row from every line, in order.
    """
    rows = []
    for line in stdout.splitlines():
        if line.strip():
            rows.extend(json.loads(line).values())  # a non-JSON line raises: never skip what we cannot read
    return rows


def missing_tables(rows: list[dict], expected: tuple[str, ...]) -> list[str]:
    """
    Tables that should have been validated but have no result row.

    Args:
        rows: Rows from run_dvt() for the count command.
        expected: Source table names, without schema.

    Returns:
        The missing names, sorted; empty when every table reported.
    """
    reported = {row["source_table_name"] for row in rows}
    return sorted(t for t in expected if f"{SOURCE_SCHEMA}.{t}" not in reported)


def failed_rows(rows: list[dict]) -> list[dict]:
    """
    Rows whose status is not success.

    Args:
        rows: Rows from run_dvt().

    Returns:
        The failing rows, in order.
    """
    return [row for row in rows if row.get("validation_status") != SUCCESS_STATUS]



def postgres_connection_args(database_url: str) -> list[str]:
    """
    DVT 'connections add' arguments for the source, from DATABASE_URL.

    Args:
        database_url: postgresql://user:password@host:port/database

    Returns:
        Arguments after the DVT executable.
    """
    url = urlsplit(database_url)
    parts = {
        "host": url.hostname,
        "user": unquote(url.username or ""),
        "password": unquote(url.password or ""),  # %21 in a URL is ! in the real password
        "database": url.path.lstrip("/"),
    }
    missing = [name for name, value in parts.items() if not value]
    if missing:
        raise ValueError(f"DATABASE_URL has no {', '.join(missing)}")
    return [
        "connections", "add", "-c", SOURCE_CONN, "Postgres",
        "--host", parts["host"], "--port", str(url.port or DEFAULT_POSTGRES_PORT),
        "--user", parts["user"], "--password", parts["password"], "--database", parts["database"],
    ]


def snowflake_connection_args(env: Mapping[str, str]) -> list[str]:
    """
    DVT 'connections add' arguments for the target, key-pair auth.

    Args:
        env: Environment with the SNOWFLAKE_* variables.

    Returns:
        Arguments after the DVT executable.
    """
    missing = [name for name in SNOWFLAKE_VARIABLES if not env.get(name)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    return [
        "connections", "add", "-c", TARGET_CONN, "Snowflake",
        "--account", env["SNOWFLAKE_ACCOUNT"], "--user", env["SNOWFLAKE_USER"],
        "--password", "",  # required by DVT's signature; the key authenticates
        "--database", f"{env['SNOWFLAKE_DATABASE']}/{TARGET_SCHEMA}",
        "--warehouse", env["SNOWFLAKE_WAREHOUSE"], "--role", env["SNOWFLAKE_ROLE"],
        "--connect-args", json.dumps({"private_key_file": env["SNOWFLAKE_PRIVATE_KEY_PATH"]}),
    ]


def add_connections(env: Mapping[str, str], conn_home: str) -> None:
    """
    Create both DVT connections in conn_home; DVT connects once to verify each.

    Args:
        env: Environment with DATABASE_URL and the SNOWFLAKE_* variables.
        conn_home: Folder for the connection files, deleted after the run.
    """
    if not env.get("DATABASE_URL"):
        raise ValueError("Missing environment variable: DATABASE_URL")
    dvt_env = {**env, CONN_HOME_VARIABLE: conn_home}
    for name, args in (
        (SOURCE_CONN, postgres_connection_args(env["DATABASE_URL"])),
        (TARGET_CONN, snowflake_connection_args(env)),
    ):
        completed = subprocess.run(_dvt(*args), capture_output=True, text=True, env=dvt_env)
        if completed.returncode != 0:  # the command holds a password, so only the name is reported
            raise RuntimeError(f"Could not create DVT connection {name}:\n{completed.stderr[-2000:]}")
