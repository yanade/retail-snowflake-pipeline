"""Generate additional days of transactional data for incremental-loading tests.

Reuses database/generate_data.py's helpers.
Targets a date range that continues after existing data instead of always starting
at 2025-01-01, so it adds new rows rather than duplicating existing ones.
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone

import psycopg

from generate_data import (
    get_database_url,
    logger,
    fetch_stores,
    fetch_products,
    fetch_employees,
    insert_order,
)

ANCHOR_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(description="Append incremental days of order data.")
    parser.add_argument(
        "--start-day",
        type=int,
        default=None,
        help="Day offset from 2025-01-01. Omit to auto-continue from the latest existing order date.",
    )
    parser.add_argument("--days", type=int, default=2, help="Number of additional days to generate.")
    parser.add_argument("--orders-per-day", type=int, default=20, help="Approximate orders per day.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatability.")
    return parser.parse_args()


def get_next_start_date(connection: psycopg.Connection) -> datetime:
    """
    Determine where to continue generating from — the day after the
    latest existing order, or ANCHOR_DATE if no orders exist yet.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        Start date for the next batch of generated orders.
    """

    with connection.cursor() as cursor:
        cursor.execute("select max(order_date::date) from retail_oltp.orders")
        (max_date,) = cursor.fetchone()

    if max_date is None:
        return ANCHOR_DATE
    return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)


def append_days(args: argparse.Namespace) -> None:
    """
    Generate and insert additional days of orders, order items, and payments.

    Args:
        args: Parsed CLI arguments.
    """

    rng = random.Random(args.seed)
    database_url = get_database_url()

    with psycopg.connect(database_url) as connection:
        start_date = (
            ANCHOR_DATE + timedelta(days=args.start_day)
            if args.start_day is not None
            else get_next_start_date(connection)
        )
        logger.info("Appending %d day(s) starting %s", args.days, start_date.date())

        stores = fetch_stores(connection)
        products = fetch_products(connection)
        employees = fetch_employees(connection)
        order_sequence = 1
        for day_offset in range(args.days):
            business_date = start_date + timedelta(days=day_offset)
            daily_orders = max(1, int(rng.normalvariate(args.orders_per_day, 8)))
            for _ in range(daily_orders):
                insert_order(connection, rng, business_date, order_sequence, stores, products, employees)
                order_sequence += 1
        connection.commit()
    logger.info("Appended %d day(s) of transactional data.", args.days)


def main() -> None:
    """Parse CLI arguments and append incremental data."""

    append_days(parse_args())


if __name__ == "__main__":
    main()
