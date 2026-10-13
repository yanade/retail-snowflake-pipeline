from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from generate_incremental_data import append_days, fetch_last_order_date, fetch_next_order_sequence

DAYS = [datetime(2026, 10, 12, tzinfo=timezone.utc), datetime(2026, 10, 13, tzinfo=timezone.utc)]

def _connection(*fetchone_results):
    """Mock connection whose cursor returns each fetchone result in turn."""

    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value.fetchone.side_effect = list(fetchone_results)
    return connection


def test_empty_source_fails_instead_of_starting_history():
    with pytest.raises(RuntimeError, match="load_database"):
        fetch_last_order_date(_connection((None,)))


def test_next_sequence_continues_after_existing_orders():
    assert fetch_next_order_sequence(_connection((4521,))) == 4521


def test_append_days_generates_catch_up_days_from_the_next_sequence():
    connection = _connection((date(2026, 10, 11),), (4521,))  # last order date, then count + 1
    args = MagicMock(orders_per_day=5, seed=1)

    with patch("generate_incremental_data.psycopg.connect", return_value=connection), \
         patch("generate_incremental_data.get_database_url", return_value="postgresql://example"), \
         patch("generate_incremental_data.fetch_stores", return_value=["store"]), \
         patch("generate_incremental_data.fetch_products", return_value=["product"]), \
         patch("generate_incremental_data.fetch_employees", return_value=["employee"]), \
         patch("generate_incremental_data.catch_up_days", return_value=DAYS) as mock_days, \
         patch("generate_incremental_data.insert_days", return_value=4531) as mock_insert_days:
        append_days(args)

    assert mock_days.call_args.args[0] == date(2026, 10, 11)
    assert mock_insert_days.call_args.args[2:4] == (DAYS, 4521)  # the days, then the first sequence
    connection.commit.assert_called_once()
