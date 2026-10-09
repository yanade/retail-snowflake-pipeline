"""Run the DVT suite for one load window and fail loudly on any mismatch (ADR-020)."""

from datetime import datetime, timezone
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent / "sql"
WINDOW_END_PLACEHOLDER = "{window_end}"  # replaced by a UTC timestamp literal in every *_source.sql


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
