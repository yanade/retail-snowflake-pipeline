from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from generate_incremental_data import ANCHOR_DATE, append_days, get_next_start_date


# ── get_next_start_date() tests ──────────────────────────────────────────────

def test_get_next_start_date_no_existing_orders():
    """
    get_next_start_date() returns ANCHOR_DATE when the orders table is empty.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (None,)
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    result = get_next_start_date(mock_connection)

    assert result == ANCHOR_DATE


def test_get_next_start_date_continues_after_latest_order():
    """
    get_next_start_date() returns the day after the latest existing order,
    so re-running the script appends rather than duplicates.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (date(2025, 2, 2),)
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    result = get_next_start_date(mock_connection)

    assert result == datetime(2025, 2, 3, tzinfo=timezone.utc)


# ── append_days() tests ──────────────────────────────────────────────────────

def _mock_connection(latest_order_date=None):
    """Build a mock psycopg connection whose cursor reports latest_order_date."""

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (latest_order_date,)
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connection.__enter__.return_value = mock_connection
    return mock_connection


def test_append_days_uses_explicit_start_day_over_auto_continue():
    """
    append_days() uses args.start_day directly when provided, instead of
    querying for the latest existing order date.
    """
    mock_connection = _mock_connection(latest_order_date=date(2025, 2, 2))
    args = MagicMock(start_day=10, days=1, orders_per_day=3, seed=1)

    with patch("generate_incremental_data.psycopg.connect", return_value=mock_connection), \
         patch("generate_incremental_data.get_database_url", return_value="postgresql://example"), \
         patch("generate_incremental_data.fetch_stores", return_value=["store"]), \
         patch("generate_incremental_data.fetch_products", return_value=["product"]), \
         patch("generate_incremental_data.fetch_employees", return_value=["employee"]), \
         patch("generate_incremental_data.insert_order") as mock_insert_order:
        append_days(args)

    expected_date = ANCHOR_DATE + timedelta(days=10)
    actual_date = mock_insert_order.call_args_list[0].args[2]
    assert actual_date == expected_date


def test_append_days_auto_continues_when_start_day_omitted():
    """
    append_days() falls back to get_next_start_date() — the day after the
    latest existing order — when args.start_day is not given.
    """
    mock_connection = _mock_connection(latest_order_date=date(2025, 2, 2))
    args = MagicMock(start_day=None, days=1, orders_per_day=3, seed=1)

    with patch("generate_incremental_data.psycopg.connect", return_value=mock_connection), \
         patch("generate_incremental_data.get_database_url", return_value="postgresql://example"), \
         patch("generate_incremental_data.fetch_stores", return_value=["store"]), \
         patch("generate_incremental_data.fetch_products", return_value=["product"]), \
         patch("generate_incremental_data.fetch_employees", return_value=["employee"]), \
         patch("generate_incremental_data.insert_order") as mock_insert_order:
        append_days(args)

    expected_date = datetime(2025, 2, 3, tzinfo=timezone.utc)
    actual_date = mock_insert_order.call_args_list[0].args[2]
    assert actual_date == expected_date


def test_append_days_inserts_per_day_and_commits():
    """
    append_days() inserts at least one order per requested day and commits
    once at the end, rather than per-order.
    """
    mock_connection = _mock_connection(latest_order_date=None)
    args = MagicMock(start_day=0, days=3, orders_per_day=5, seed=42)

    with patch("generate_incremental_data.psycopg.connect", return_value=mock_connection), \
         patch("generate_incremental_data.get_database_url", return_value="postgresql://example"), \
         patch("generate_incremental_data.fetch_stores", return_value=["store"]), \
         patch("generate_incremental_data.fetch_products", return_value=["product"]), \
         patch("generate_incremental_data.fetch_employees", return_value=["employee"]), \
         patch("generate_incremental_data.insert_order") as mock_insert_order:
        append_days(args)

    business_dates = {call.args[2] for call in mock_insert_order.call_args_list}
    assert business_dates == {
        ANCHOR_DATE,
        ANCHOR_DATE + timedelta(days=1),
        ANCHOR_DATE + timedelta(days=2),
    }
    mock_connection.commit.assert_called_once()
