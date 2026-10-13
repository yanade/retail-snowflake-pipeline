"""Simulate source activity since the last order: new orders for every day up to today.

Reuses the shared generator modules: lookups, order_writer, runtime and sales_calendar.
Run load_database.sh first; this script continues existing history, it does not start one.
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import date, datetime, timezone

import psycopg

from lookups import fetch_employees, fetch_products, fetch_stores
from order_writer import insert_days
from runtime import get_database_url, setup_logging
from sales_calendar import catch_up_days

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(description="Append orders for every day since the last order, up to today.")
    parser.add_argument("--orders-per-day", type=int, default=20, help="Approximate orders per day.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatability.")
    return parser.parse_args()


def fetch_last_order_date(connection: psycopg.Connection) -> date:
    """
    Return the UTC date of the newest order.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        The newest order's date.

    Raises:
        RuntimeError: If there are no orders, so there is no history to continue.
    """

    with connection.cursor() as cursor:
        cursor.execute("select max((order_date at time zone 'UTC')::date) from retail_oltp.orders")
        (last_order_date,) = cursor.fetchone()
    if last_order_date is None:
        raise RuntimeError("No orders yet. Run scripts/load_database.sh first.")
    return last_order_date


def fetch_next_order_sequence(connection: psycopg.Connection) -> int:
    """
    Return the next order number sequence, continuing generate_data's numbering.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        Order count plus one; generate_data numbers every order 1..N, duplicates included.
    """

    with connection.cursor() as cursor:
        cursor.execute("select count(*) + 1 from retail_oltp.orders")
        (next_sequence,) = cursor.fetchone()
    return next_sequence


def append_days(args: argparse.Namespace) -> None:
    """
    Insert orders for every day since the last order, up to today.

    Args:
        args: Parsed CLI arguments.
    """

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)  # one moment for the whole run, so every cap agrees
    database_url = get_database_url()

    with psycopg.connect(database_url) as connection:
        days = catch_up_days(fetch_last_order_date(connection), now)
        first_sequence = fetch_next_order_sequence(connection)
        logger.info("Appending %d day(s) from %s to %s, orders from #%d",
                    len(days), days[0].date(), days[-1].date(), first_sequence)

        stores = fetch_stores(connection)
        products = fetch_products(connection)
        employees = fetch_employees(connection)
        next_sequence = insert_days(connection, rng, days, first_sequence, stores, products, employees,
                                    args.orders_per_day, now)
        connection.commit()
    logger.info("Appended %d order(s).", next_sequence - first_sequence)


def main() -> None:
    """Parse CLI arguments and append incremental data."""

    setup_logging()
    append_days(parse_args())


if __name__ == "__main__":
    main()