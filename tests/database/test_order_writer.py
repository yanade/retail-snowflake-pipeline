import random
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from lookups import Product, Store
from order_writer import insert_order

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
