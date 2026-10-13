"""Reference data the generators read from retail_oltp: stores, products, employees."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import psycopg


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
