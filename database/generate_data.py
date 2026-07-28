"""Generate realistic transactional data for the retail OLTP database."""

from __future__ import annotations

import argparse
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv


def setup_logging() -> logging.Logger:
    """
    Configure and return a shared logger.

    Duplicated from utils/logger.py rather than imported — this script is
    run as `cd database && python generate_data.py`, not
    `python -m database.generate_data` from the project root, so `utils`
    isn't importable here.

    Returns:
        Configured root logger.
    """

    if logging.getLogger().handlers:
        return logging.getLogger()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger()


logger = setup_logging()


COUNTRIES = ["GB", "DE", "FR", "US", "CA", "AU", None]
FIRST_NAMES = ["Ava", "Oliver", "Sophia", "George", "Isla", "Theo", "Maya", "Leo"]
LAST_NAMES = ["Taylor", "Brown", "Wilson", "Davies", "Evans", "Khan", "Murphy", "Hall"]
ORDER_STATUSES = ["PENDING", "PAID", "SHIPPED", "COMPLETED", "CANCELLED", "RETURNED"]
PAYMENT_METHODS = ["CARD", "PAYPAL", "APPLE_PAY", "BANK_TRANSFER", "GIFT_CARD"]
PAYMENT_STATUSES = ["PENDING", "AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED", "PARTIALLY_REFUNDED", "SETTLED_UNKNOWN"]


@dataclass(frozen=True)
class Store:
    """Store lookup used for generated orders."""

    store_id: int
    store_type: str
    currency_code: str


@dataclass(frozen=True)
class Product:
    """Product lookup used for generated order lines."""

    product_id: int
    sku: str
    standard_unit_price: Decimal


@dataclass(frozen=True)
class Employee:
    """Employee lookup used for generated orders."""

    employee_id: int
    store_id: int


def get_database_url() -> str:
    """
    Return the PostgreSQL connection string.

    Returns:
        The PostgreSQL connection URL from the DATABASE_URL environment variable.

    Raises:
        RuntimeError: if DATABASE_URL is not set.
    """

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Set it explicitly — e.g. "
            "postgresql://retail_user:retail_password@localhost:5432/retail_source "
            "for local Docker, or the Azure PostgreSQL connection string for the cloud instance."
        )
    return database_url


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(description="Generate retail OLTP data.")
    parser.add_argument("--days", type=int, default=30, help="Number of sales days to generate.")
    parser.add_argument("--orders-per-day", type=int, default=50, help="Approximate orders per day.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatability.")
    return parser.parse_args()


def fetch_stores(connection: psycopg.Connection) -> list[Store]:
    """
    Fetch stores for assigning orders.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        Store lookup rows.

    Raises:
        RuntimeError: if stores have not been seeded.
    """

    currency_by_country = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "US": "USD", "CA": "CAD"}
    with connection.cursor() as cursor:
        cursor.execute("select store_id, store_type, country_code from retail_oltp.stores order by store_id")
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError("No stores found. Run seed.py first.")
    return [Store(row[0], row[1], currency_by_country.get(row[2], "GBP")) for row in rows]


def fetch_products(connection: psycopg.Connection) -> list[Product]:
    """
    Fetch products for generated order lines.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        Product lookup rows.

    Raises:
        RuntimeError: if products have not been seeded.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            """
            select product_id, sku, coalesce(standard_unit_price, 1.00)
            from retail_oltp.products
            order by product_id
            """
        )
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError("No products found. Run seed.py first.")
    return [Product(row[0], row[1], row[2]) for row in rows]


def fetch_employees(connection: psycopg.Connection) -> list[Employee]:
    """
    Fetch employees for generated orders.

    Args:
        connection: Open PostgreSQL connection.

    Returns:
        Employee lookup rows.
    """

    with connection.cursor() as cursor:
        cursor.execute("select employee_id, store_id from retail_oltp.employees order by employee_id")
        rows = cursor.fetchall()
    return [Employee(row[0], row[1]) for row in rows]


def money(value: Decimal) -> Decimal:
    """
    Round a monetary value to two decimal places.

    Args:
        value: Decimal amount.

    Returns:
        Rounded Decimal amount.
    """

    return value.quantize(Decimal("0.01"))


def insert_customer(connection: psycopg.Connection, rng: random.Random) -> int | None:
    """
    Insert a customer or return None for guest checkout.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.

    Returns:
        Customer primary key or None.
    """

    if rng.random() < 0.08:
        return None
    first_name = rng.choice(FIRST_NAMES)
    last_name = rng.choice(LAST_NAMES)
    email_name = "shared.customer" if rng.random() < 0.04 else f"{first_name}.{last_name}.{rng.randint(1, 9999)}"
    email = None if rng.random() < 0.06 else f"{email_name.lower()}@example.com"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into retail_oltp.customers
                (customer_number, first_name, last_name, email, phone, country_code, customer_status)
            values
                (%s, %s, %s, %s, %s, %s, %s)
            returning customer_id
            """,
            (
                f"CUST-{rng.randint(100000, 999999)}",
                None if rng.random() < 0.03 else first_name,
                None if rng.random() < 0.03 else last_name,
                email,
                None if rng.random() < 0.15 else f"+44{rng.randint(7000000000, 7999999999)}",
                rng.choice(COUNTRIES),
                rng.choice(["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]),
            ),
        )
        customer_id = cursor.fetchone()[0]
    return customer_id


def choose_employee(rng: random.Random, store: Store, employees: Sequence[Employee]) -> int | None:
    """
    Choose an employee for physical-store orders.

    Args:
        rng: Random number generator.
        store: Selected store.
        employees: Available employees.

    Returns:
        Employee ID or None.
    """

    if store.store_type == "ONLINE" or rng.random() < 0.1:
        return None
    store_employees = [employee for employee in employees if employee.store_id == store.store_id]
    if not store_employees:
        return None
    return rng.choice(store_employees).employee_id


def insert_order_items(
    connection: psycopg.Connection,
    rng: random.Random,
    order_id: int,
    products: Sequence[Product],
) -> Decimal:
    """
    Insert order item rows for an order.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.
        order_id: Parent order ID.
        products: Product lookup rows.

    Returns:
        Calculated subtotal.
    """

    subtotal = Decimal("0.00")
    with connection.cursor() as cursor:
        for line_number in range(1, rng.randint(1, 5) + 1):
            product = rng.choice(products)
            quantity = rng.choice([-2, -1, 0, 1, 1, 1, 2, 3, 4])
            unit_price = money(product.standard_unit_price * Decimal(str(rng.uniform(0.85, 1.15))))
            line_discount = money(unit_price * abs(quantity) * Decimal(str(rng.choice([0, 0, 0.05]))))
            line_tax = money((unit_price * quantity - line_discount) * Decimal("0.20"))
            line_total = money(unit_price * quantity - line_discount + line_tax)
            subtotal += money(unit_price * quantity)
            unresolved_product = rng.random() < 0.025
            cursor.execute(
                """
                insert into retail_oltp.order_items
                    (order_id, line_number, product_id, source_product_sku, quantity, unit_price, discount_amount, tax_amount, line_total_amount)
                values
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    line_number,
                    None if unresolved_product else product.product_id,
                    f"UNKNOWN-{rng.randint(1000, 9999)}" if unresolved_product else product.sku,
                    quantity,
                    unit_price,
                    line_discount,
                    line_tax,
                    line_total,
                ),
            )
    return money(subtotal)


def insert_payment(
    connection: psycopg.Connection,
    rng: random.Random,
    order_id: int,
    order_date: datetime,
    total: Decimal,
    currency_code: str,
    order_status: str,
) -> None:
    """
    Insert payment rows for an order.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.
        order_id: Parent order ID.
        order_date: Parent order timestamp.
        total: Order total amount.
        currency_code: Payment currency.
        order_status: Parent order status.
    """

    if order_status == "CANCELLED":
        status = rng.choice(["FAILED", "REFUNDED", "AUTHORIZED"])
    elif order_status == "RETURNED":
        status = rng.choice(["REFUNDED", "PARTIALLY_REFUNDED", "CAPTURED"])
    else:
        status = rng.choices(PAYMENT_STATUSES, weights=[3, 8, 70, 5, 4, 2, 1])[0]
    amount = Decimal("0.00") if status == "FAILED" else total
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into retail_oltp.payments
                (order_id, payment_reference, payment_method, payment_status, payment_amount, currency_code, payment_date)
            values
                (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                f"PAY-{order_id}-{rng.randint(1000, 9999)}",
                rng.choice(PAYMENT_METHODS),
                status,
                amount,
                currency_code,
                order_date + timedelta(minutes=rng.randint(1, 30)),
            ),
        )


def insert_order(
    connection: psycopg.Connection,
    rng: random.Random,
    business_date: datetime,
    order_sequence: int,
    stores: Sequence[Store],
    products: Sequence[Product],
    employees: Sequence[Employee],
) -> int:
    """
    Insert one order with lines and payment.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.
        business_date: Date used for the order timestamp.
        order_sequence: Sequence number used for order numbers.
        stores: Store lookup rows.
        products: Product lookup rows.
        employees: Employee lookup rows.

    Returns:
        Inserted order ID.
    """

    store = rng.choice(stores)
    order_date = business_date + timedelta(minutes=rng.randint(0, 1439))
    order_number = (
        f"ORD-{business_date:%Y%m%d}-DUPLICATE"
        if rng.random() < 0.015
        else f"ORD-{business_date:%Y%m%d}-{order_sequence:06d}"
    )
    status = rng.choices(ORDER_STATUSES, weights=[4, 20, 20, 45, 7, 4])[0]
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into retail_oltp.orders
                (order_number, customer_id, store_id, employee_id, order_status, order_date, currency_code, subtotal_amount, tax_amount, shipping_amount, discount_amount, total_amount)
            values
                (%s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0)
            returning order_id
            """,
            (
                order_number,
                insert_customer(connection, rng),
                store.store_id,
                choose_employee(rng, store, employees),
                status,
                order_date,
                store.currency_code,
            ),
        )
        order_id = cursor.fetchone()[0]
    subtotal = insert_order_items(connection, rng, order_id, products)
    discount = money(subtotal * Decimal(str(rng.choice([0, 0, 0.05, 0.10]))))
    tax = money((subtotal - discount) * Decimal("0.20"))
    shipping = Decimal("0.00") if subtotal > Decimal("50.00") else Decimal("4.99")
    total = money(subtotal - discount + tax + shipping)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            update retail_oltp.orders
            set subtotal_amount = %s, tax_amount = %s, shipping_amount = %s, discount_amount = %s, total_amount = %s
            where order_id = %s
            """,
            (subtotal, tax, shipping, discount, total, order_id),
        )
    insert_payment(connection, rng, order_id, order_date, total, store.currency_code, status)
    return order_id


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
    start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)

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
        order_sequence = 1
        for day_offset in range(args.days):
            business_date = start_date + timedelta(days=day_offset)
            daily_orders = max(1, int(rng.normalvariate(args.orders_per_day, 8)))
            for _ in range(daily_orders):
                insert_order(connection, rng, business_date, order_sequence, stores, products, employees)
                order_sequence += 1
        apply_late_arriving_updates(
            connection,
            rng,
            update_count=max(1, int(args.days * args.orders_per_day * 0.03)),
        )
        connection.commit()
    logger.info("Transactional data generated successfully.")


def main() -> None:
    """Parse CLI arguments and generate data."""

    generate_data(parse_args())


if __name__ == "__main__":
    main()
