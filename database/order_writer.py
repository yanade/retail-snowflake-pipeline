"""Insert one realistic order, with its customer, lines and payment, including the deliberate data quality issues."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Sequence

import psycopg

from lookups import Employee, Product, Store

COUNTRIES = ["GB", "DE", "FR", "US", "CA", "AU", None]
FIRST_NAMES = ["Ava", "Oliver", "Sophia", "George", "Isla", "Theo", "Maya", "Leo"]
LAST_NAMES = ["Taylor", "Brown", "Wilson", "Davies", "Evans", "Khan", "Murphy", "Hall"]
ORDER_STATUSES = ["PENDING", "PAID", "SHIPPED", "COMPLETED", "CANCELLED", "RETURNED"]
PAYMENT_METHODS = ["CARD", "PAYPAL", "APPLE_PAY", "BANK_TRANSFER", "GIFT_CARD"]
PAYMENT_STATUSES = ["PENDING", "AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED", "PARTIALLY_REFUNDED", "SETTLED_UNKNOWN"]
STREET_NAMES = ["Oak Lane", "Mill Road", "Station Street", "Harbour View", "Elm Close", "Kings Way"]
# Cities per country so a customer's address is geographically consistent
# with their country_code, rather than a random pairing like GB / Sydney.
CITIES_BY_COUNTRY = {
    "GB": ["London", "Manchester", "Bristol"],
    "DE": ["Berlin", "Hamburg", "Munich"],
    "FR": ["Paris", "Lyon", "Nantes"],
    "US": ["Austin", "Denver", "Seattle"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "AU": ["Sydney", "Melbourne", "Perth"],
}
# Used when the customer's country_code is NULL, which is a deliberate
# data quality case in COUNTRIES above.
FALLBACK_CITIES = ["London", "Berlin", "Toronto"]


def money(value: Decimal) -> Decimal:
    """
    Round a monetary value to two decimal places.

    Args:
        value: Decimal amount.

    Returns:
        Rounded Decimal amount.
    """

    return value.quantize(Decimal("0.01"))


def insert_customer_address(
    connection: psycopg.Connection,
    rng: random.Random,
    customer_id: int,
    country_code: str | None,
) -> None:
    """
    Insert one or two addresses for a newly created customer.

    Only called for registered customers. Guest checkouts return before
    this point, which is what customer_addresses.customer_id being NOT NULL
    requires.

    Args:
        connection: Open PostgreSQL connection.
        rng: Random number generator.
        customer_id: Owning customer primary key.
        country_code: The customer's country, so the city matches it.
    """

    # Most customers ship and bill to the same place. A minority keep a
    # separate billing address, which is what makes address_type meaningful.
    address_types = ["SHIPPING"] if rng.random() < 0.75 else ["SHIPPING", "BILLING"]
    cities = CITIES_BY_COUNTRY.get(country_code, FALLBACK_CITIES)
    with connection.cursor() as cursor:
        for position, address_type in enumerate(address_types):
            cursor.execute(
                """
                insert into retail_oltp.customer_addresses
                    (customer_id, address_type, address_line_1, address_line_2,
                     city, region, postal_code, country_code, is_default)
                values
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    customer_id,
                    address_type,
                    f"{rng.randint(1, 240)} {rng.choice(STREET_NAMES)}",
                    # Second address line is genuinely optional in the domain,
                    # so it is left NULL most of the time rather than padded.
                    f"Flat {rng.randint(1, 40)}" if rng.random() < 0.2 else None,
                    rng.choice(cities),
                    None if rng.random() < 0.3 else rng.choice(["Greater London", "Bavaria", "Ontario", "Ile-de-France"]),
                    f"{rng.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}{rng.randint(1, 99)} {rng.randint(1, 9)}{rng.choice('ABDEFGHJLNPQRSTUWXYZ')}{rng.choice('ABDEFGHJLNPQRSTUWXYZ')}",
                    country_code,
                    # Exactly one default per customer. Without this the
                    # dimension has no way to pick a primary address.
                    position == 0,
                ),
            )


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
    # Hoisted out of the insert tuple so the address below can reuse it.
    # NULL is a deliberate data quality case, see COUNTRIES.
    country_code = rng.choice(COUNTRIES)
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
                country_code,
                rng.choice(["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]),
            ),
        )
        customer_id = cursor.fetchone()[0]
    insert_customer_address(connection, rng, customer_id, country_code)
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
