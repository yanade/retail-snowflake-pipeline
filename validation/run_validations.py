"""Run the DVT suite for one load window and fail loudly on any mismatch (ADR-020)."""
import sys
from dataclasses import dataclass

from datetime import datetime, timezone
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent / "sql"
WINDOW_END_PLACEHOLDER = "{window_end}"  # replaced by a UTC timestamp literal in every *_source.sql
SOURCE_CONN = "pg_source"
TARGET_CONN = "sf_dev"
SOURCE_SCHEMA = "retail_oltp"
TARGET_SCHEMA = "DBT_DEV_STAGING"  # upper case, or ibis quotes the name and Snowflake cannot find it

COUNT_TABLES = (  # no reject rules, so source and staging must have the same row count
    "currencies", "product_categories", "suppliers", "products", "customers",
    "customer_addresses", "stores", "employees", "exchange_rates",
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
