"""Generate realistic transactional data for the retail OLTP database."""

from __future__ import annotations

import argparse
import logging
import random
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import psycopg

from lookups import fetch_employees, fetch_products, fetch_stores
from order_writer import insert_days
from runtime import get_database_url, setup_logging
from sales_calendar import business_days, resolve_end_date

logger = logging.getLogger(__name__)  # configured by setup_logging() in main, not at import


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(description="Generate retail OLTP data.")
    parser.add_argument("--days", type=int, default=90, help="Number of sales days to generate.")
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,  # argparse calls it on the string, so a bad date fails at parse time
        default=None,
        help="Last sales day, YYYY-MM-DD. Defaults to today in UTC.",
    )
    parser.add_argument("--orders-per-day", type=int, default=50, help="Approximate orders per day.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatability.")
    return parser.parse_args()


def apply_late_arriving_updates(connection: psycopg.Connection, rng: random.Random, update_count: int) -> None:
    """
    Update older orders so updated_at changes after insert.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.
        update_count: Number of orders to update.
    """

    with connection.cursor() as cursor:
        cursor.execute("select order_id from retail_oltp.orders order by random() limit %s", (update_count,))
        order_ids = [row[0] for row in cursor.fetchall()]
        for order_id in order_ids:
            cursor.execute(
                "update retail_oltp.orders set order_status = %s where order_id = %s",
                (rng.choice(["SHIPPED", "COMPLETED", "CANCELLED", "RETURNED"]), order_id),
            )


def generate_data(args: argparse.Namespace) -> None:
    """
    Generate customers, orders, order items, payments, and updates.

    Args:
        args: Parsed CLI arguments.
    """

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)  # one moment for the whole run, so every cap agrees
    days = business_days(resolve_end_date(args.end_date, now), args.days)

    database_url = get_database_url()
    parsed = urlparse(database_url)
    logger.info(
        "Connecting to host=%s database=%s schema=retail_oltp",
        parsed.hostname,
        parsed.path.lstrip("/"),
    )

    with psycopg.connect(database_url) as connection:
        stores = fetch_stores(connection)
        products = fetch_products(connection)
        employees = fetch_employees(connection)
        
        logger.info("Generating %d day(s) from %s to %s", len(days), days[0].date(), days[-1].date())

        insert_days(connection, rng, days, 1, stores, products, employees, args.orders_per_day, now)
        apply_late_arriving_updates(
            connection,
            rng,
            update_count=max(1, int(args.days * args.orders_per_day * 0.03)),
        )
        connection.commit()
    logger.info("Transactional data generated successfully.")


def main() -> None:
    """Parse CLI arguments and generate data."""

    setup_logging()
    generate_data(parse_args())


if __name__ == "__main__":
    main()
