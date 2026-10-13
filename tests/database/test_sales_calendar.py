from datetime import date, datetime, timezone
import random

import pytest

from sales_calendar import business_days, order_timestamp, resolve_end_date

NOW = datetime(2026, 10, 13, 10, 0, tzinfo=timezone.utc)


def test_window_ends_today_and_spans_90_days():
    days = business_days(resolve_end_date(None, NOW), 90)
    assert (len(days), days[0].date(), days[-1].date()) == (90, date(2026, 7, 16), date(2026, 10, 13))


def test_future_end_date_fails():
    with pytest.raises(ValueError):
        resolve_end_date(date(2026, 10, 14), NOW)


def test_today_orders_never_after_now():
    rng = random.Random(1)
    midnight = datetime(2026, 10, 13, tzinfo=timezone.utc)
    assert all(order_timestamp(rng, midnight, NOW) <= NOW for _ in range(1000))


def test_past_day_uses_the_whole_day():
    rng = random.Random(1)
    yesterday = datetime(2026, 10, 12, tzinfo=timezone.utc)
    assert max(order_timestamp(rng, yesterday, NOW) for _ in range(5000)).hour >= 22
