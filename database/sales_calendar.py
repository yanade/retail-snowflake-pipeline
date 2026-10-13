"""When sales happen: the day window ending on a given date, and order times that never pass the run's clock."""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta, timezone

MINUTES_PER_DAY = 1440


def resolve_end_date(end_date: date | None, now: datetime) -> date:
    """
    Return the last sales day: the given date, or today in UTC.

    Args:
        end_date: Requested last day, or None for today.
        now: The run's clock, UTC-aware.

    Returns:
        The last sales day.

    Raises:
        ValueError: If end_date is after today.
    """

    today = now.date()  # now is UTC-aware, so this is the UTC date
    if end_date is None:
        return today
    if end_date > today:
        raise ValueError(f"--end-date {end_date} is after today {today}; a source cannot hold future sales")
    return end_date


def business_days(end_date: date, days: int) -> list[datetime]:
    """
    Return midnight UTC of each sales day, oldest first, ending on end_date.

    Args:
        end_date: Last sales day, included.
        days: Number of days in the window.

    Returns:
        One timezone-aware datetime per day.

    Raises:
        ValueError: If days is less than 1.
    """

    if days < 1:
        raise ValueError(f"--days must be at least 1, got {days}")
    first = end_date - timedelta(days=days - 1)  # inclusive: 90 days end on end_date
    return [datetime.combine(first + timedelta(days=offset), time.min, tzinfo=timezone.utc) for offset in range(days)]


def order_timestamp(rng: random.Random, business_date: datetime, now: datetime) -> datetime:
    """
    Return a random moment in the sales day, never after now.

    Args:
        rng: Random number generator.
        business_date: Midnight UTC of the sales day.
        now: The run's clock, UTC-aware.

    Returns:
        The order timestamp.
    """

    elapsed = int((now - business_date).total_seconds() // 60)  # minutes of the day already passed
    return business_date + timedelta(minutes=rng.randint(0, min(MINUTES_PER_DAY - 1, elapsed)))
