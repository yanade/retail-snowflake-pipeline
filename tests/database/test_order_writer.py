import random
from datetime import datetime, timezone, date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from lookups import Product, Store
from order_writer import insert_order, insert_days


NOW = datetime(2026, 10, 13, 10, 0, tzinfo=timezone.utc)
MIDNIGHT = datetime(2026, 10, 13, tzinfo=timezone.utc)


def test_order_and_payment_are_never_after_now():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (1,)  # every "returning ..._id" gets id 1
    rng = random.Random(1)

    for _ in range(200):
        insert_order(connection, rng, MIDNIGHT, 1, [Store(1, "ONLINE", "GBP", None, None)],
                     [Product(1, "SKU-1", Decimal("10.00"))], [], NOW)

    timestamps = [arg for call in cursor.execute.call_args_list
                  for arg in (call.args[1] if len(call.args) > 1 else ())
                  if isinstance(arg, datetime)]  # order_date and payment_date from every insert
    assert timestamps and all(ts <= NOW for ts in timestamps)


def test_insert_days_continues_numbering_and_skips_closed_stores():
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (1,)
    closed = Store(5, "OUTLET", "EUR", date(2017, 5, 12), date(2025, 12, 31))
    open_store = Store(1, "ONLINE", "GBP", None, None)

    with patch("order_writer.insert_order") as mock_insert_order:
        next_sequence = insert_days(connection, random.Random(1), [MIDNIGHT], 101, [closed, open_store],
                                    [Product(1, "SKU-1", Decimal("10.00"))], [], 5, NOW)

    calls = mock_insert_order.call_args_list
    assert [call.args[3] for call in calls] == list(range(101, next_sequence))  # numbering continues from 101
    assert all(call.args[4] == [open_store] for call in calls)                  # the closed store never trades