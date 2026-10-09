"""Unit tests for validation/run_validations.py: window parsing and source query rendering."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from validation.run_validations import (
    SQL_DIR,
    WINDOW_END_PLACEHOLDER,
    parse_window_end,
    render_source_query,
    sql_timestamp,
)

WINDOW_END = datetime(2026, 10, 3, 15, 25, 24, tzinfo=timezone.utc)


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
