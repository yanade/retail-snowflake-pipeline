"""Seed reference and master data for the retail OLTP database."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, Mapping
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv


def setup_logging() -> logging.Logger:
    """
    Configure and return a shared logger.

    Duplicated from utils/logger.py rather than imported — this script is
    run as `cd database && python seed.py`, not `python -m database.seed`
    from the project root, so `utils` isn't importable here.

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


def execute_many(
    connection: psycopg.Connection, sql: str, rows: Iterable[Mapping[str, object]]
) -> None:
    """
    Execute one SQL statement for many dictionaries.

    Args:
        connection: Open PostgreSQL connection.
        sql: SQL statement with named placeholders.
        rows: Row dictionaries to bind.
    """

    with connection.cursor() as cursor:
        cursor.executemany(sql, list(rows))


def seed_currencies(connection: psycopg.Connection) -> None:
    """
    Insert supported trading currencies.

    Args:
        connection: Open PostgreSQL connection.
    """

    rows = [
        {"currency_code": "GBP", "currency_name": "British Pound Sterling", "currency_symbol": "GBP", "decimal_places": 2},
        {"currency_code": "USD", "currency_name": "US Dollar", "currency_symbol": "USD", "decimal_places": 2},
        {"currency_code": "EUR", "currency_name": "Euro", "currency_symbol": "EUR", "decimal_places": 2},
        {"currency_code": "CAD", "currency_name": "Canadian Dollar", "currency_symbol": "CAD", "decimal_places": 2},
        {"currency_code": "AUD", "currency_name": "Australian Dollar", "currency_symbol": "AUD", "decimal_places": 2},
    ]
    execute_many(
        connection,
        """
        insert into retail_oltp.currencies
            (currency_code, currency_name, currency_symbol, decimal_places)
        values
            (%(currency_code)s, %(currency_name)s, %(currency_symbol)s, %(decimal_places)s)
        on conflict (currency_code) do update
        set
            currency_name = excluded.currency_name,
            currency_symbol = excluded.currency_symbol,
            decimal_places = excluded.decimal_places
        """,
        rows,
    )


def seed_categories(connection: psycopg.Connection) -> None:
    """
    Insert product categories.

    Args:
        connection: Open PostgreSQL connection.
    """

    rows = [
        {"category_code": "HOME", "category_name": "Home"},
        {"category_code": "FASHION", "category_name": "Fashion"},
        {"category_code": "BEAUTY", "category_name": "Beauty"},
        {"category_code": "ELECTRONICS", "category_name": "Electronics"},
        {"category_code": "HOME-KITCHEN", "category_name": "Kitchen"},
        {"category_code": "HOME-DECOR", "category_name": "Decor"},
        {"category_code": "FASHION-WOMEN", "category_name": "Womenswear"},
        {"category_code": "FASHION-MEN", "category_name": "Menswear"},
        {"category_code": "BEAUTY-SKIN", "category_name": "Skincare"},
        {"category_code": "ELEC-AUDIO", "category_name": "Audio"},
    ]
    execute_many(
        connection,
        """
        insert into retail_oltp.product_categories
            (category_code, category_name)
        values
            (%(category_code)s, %(category_name)s)
        on conflict (category_code) do update
        set category_name = excluded.category_name
        """,
        rows,
    )


def seed_suppliers(connection: psycopg.Connection) -> None:
    """
    Insert suppliers.

    Args:
        connection: Open PostgreSQL connection.
    """

    rows = [
        {"supplier_code": "SUP-UK-001", "supplier_name": "Northbridge Home Goods", "country_code": "GB", "contact_email": "ops@northbridge.example"},
        {"supplier_code": "SUP-DE-014", "supplier_name": "Rhine Lifestyle GmbH", "country_code": "DE", "contact_email": "sales@rhine-lifestyle.example"},
        {"supplier_code": "SUP-US-022", "supplier_name": "Pacific Audio Supply", "country_code": "US", "contact_email": "accounts@pacific-audio.example"},
        {"supplier_code": "SUP-FR-009", "supplier_name": "Maison Lumiere", "country_code": "FR", "contact_email": None},
    ]
    execute_many(
        connection,
        """
        insert into retail_oltp.suppliers
            (supplier_code, supplier_name, country_code, contact_email)
        values
            (%(supplier_code)s, %(supplier_name)s, %(country_code)s, %(contact_email)s)
        on conflict (supplier_code) do update
        set
            supplier_name = excluded.supplier_name,
            country_code = excluded.country_code,
            contact_email = excluded.contact_email
        """,
        rows,
    )


def seed_stores(connection: psycopg.Connection) -> None:
    """
    Insert stores and channels.

    Args:
        connection: Open PostgreSQL connection.
    """

    rows = [
        {"store_code": "WEB-UK", "store_name": "UK Web Store", "store_type": "ONLINE", "country_code": "GB", "city": "London", "opened_date": date(2018, 4, 1), "closed_date": None},
        {"store_code": "LON-001", "store_name": "London Oxford Street", "store_type": "PHYSICAL", "country_code": "GB", "city": "London", "opened_date": date(2016, 9, 15), "closed_date": None},
        {"store_code": "BER-001", "store_name": "Berlin Mitte", "store_type": "PHYSICAL", "country_code": "DE", "city": "Berlin", "opened_date": date(2019, 3, 20), "closed_date": None},
        {"store_code": "TOR-001", "store_name": "Toronto Queen Street", "store_type": "PHYSICAL", "country_code": "CA", "city": "Toronto", "opened_date": date(2020, 8, 8), "closed_date": None},
        {"store_code": "PAR-OUT", "store_name": "Paris Outlet", "store_type": "OUTLET", "country_code": "FR", "city": "Paris", "opened_date": date(2017, 5, 12), "closed_date": date(2025, 12, 31)},
    ]
    execute_many(
        connection,
        """
        insert into retail_oltp.stores
            (store_code, store_name, store_type, country_code, city, opened_date, closed_date)
        values
            (%(store_code)s, %(store_name)s, %(store_type)s, %(country_code)s, %(city)s, %(opened_date)s, %(closed_date)s)
        on conflict (store_code) do update
        set
            store_name = excluded.store_name,
            store_type = excluded.store_type,
            country_code = excluded.country_code,
            city = excluded.city,
            opened_date = excluded.opened_date,
            closed_date = excluded.closed_date
        """,
        rows,
    )


def seed_products(connection: psycopg.Connection) -> None:
    """
    Insert product master data.

    Args:
        connection: Open PostgreSQL connection.
    """

    rows = [
        {"sku": "SKU-HOME-001", "product_name": "Ceramic Dinner Plate", "price": Decimal("8.50"), "currency": "GBP"},
        {"sku": "SKU-HOME-002", "product_name": "Cotton Tea Towel", "price": Decimal("4.25"), "currency": "GBP"},
        {"sku": "SKU-HOME-003", "product_name": "Glass Table Lamp", "price": Decimal("39.99"), "currency": "EUR"},
        {"sku": "SKU-FASH-010", "product_name": "Linen Shirt", "price": Decimal("34.00"), "currency": "EUR"},
        {"sku": "SKU-FASH-011", "product_name": "Wool Scarf", "price": Decimal("22.50"), "currency": "EUR"},
        {"sku": "SKU-BEAU-020", "product_name": "Vitamin C Serum", "price": Decimal("18.75"), "currency": "GBP"},
        {"sku": "SKU-ELEC-030", "product_name": "Wireless Earbuds", "price": Decimal("69.99"), "currency": "USD"},
        {"sku": "SKU-ELEC-031", "product_name": "Bluetooth Speaker", "price": Decimal("54.95"), "currency": "USD"},
        {"sku": "SKU-DUP-001", "product_name": "Duplicate SKU Test Product A", "price": Decimal("12.00"), "currency": "GBP"},
        {"sku": "SKU-DUP-001", "product_name": "Duplicate SKU Test Product B", "price": Decimal("13.00"), "currency": "GBP"},
    ]
    execute_many(
        connection,
        """
        insert into retail_oltp.products
            (sku, product_name, standard_unit_price, default_currency_code)
        values
            (%(sku)s, %(product_name)s, %(price)s, %(currency)s)
        """,
        rows,
    )


def seed_demo_exchange_rates(connection: psycopg.Connection) -> None:
    """
    Insert demo exchange rates with intentional gaps.

    Offline/no-API-key fallback only — not run by default. Real
    exchange_rates data is populated by
    ingestion/api_ingest/fetch_fx_rates.py against the live
    freecurrencyapi.com API, which also produces natural "missing rate"
    gaps for dates that haven't been fetched yet.

    Args:
        connection: Open PostgreSQL connection.
    """

    start_date = date(2025, 1, 1)
    pairs = {
        ("GBP", "USD"): Decimal("1.27000000"),
        ("EUR", "USD"): Decimal("1.09000000"),
        ("CAD", "USD"): Decimal("0.74000000"),
        ("AUD", "USD"): Decimal("0.66000000"),
        ("USD", "GBP"): Decimal("0.79000000"),
        ("EUR", "GBP"): Decimal("0.86000000"),
    }
    rows = []
    for day_offset in range(120):
        if day_offset in {13, 37, 88}:
            continue
        rate_date = start_date + timedelta(days=day_offset)
        for (base_currency, target_currency), rate in pairs.items():
            rows.append(
                {
                    "rate_date": rate_date,
                    "base_currency_code": base_currency,
                    "target_currency_code": target_currency,
                    "exchange_rate": rate,
                    "source_system": "seeded_reference",
                }
            )
    execute_many(
        connection,
        """
        insert into retail_oltp.exchange_rates
            (rate_date, base_currency_code, target_currency_code, exchange_rate, source_system)
        values
            (%(rate_date)s, %(base_currency_code)s, %(target_currency_code)s, %(exchange_rate)s, %(source_system)s)
        on conflict (rate_date, base_currency_code, target_currency_code) do update
        set
            exchange_rate = excluded.exchange_rate,
            source_system = excluded.source_system
        """,
        rows,
    )


def run_seed_steps(connection: psycopg.Connection) -> None:
    """
    Run seed steps in dependency order.

    Master/reference data only — exchange_rates is intentionally not
    seeded here. See seed_demo_exchange_rates() for the offline fallback.

    Args:
        connection: Open PostgreSQL connection.
    """

    seed_currencies(connection)
    seed_categories(connection)
    seed_suppliers(connection)
    seed_stores(connection)
    seed_products(connection)


def main() -> None:
    """Connect to PostgreSQL and seed data."""

    parser = argparse.ArgumentParser(description="Seed the retail OLTP database.")
    parser.add_argument(
        "--with-demo-fx",
        action="store_true",
        default=False,
        help=(
            "Also seed fake exchange rates with intentional gaps. "
            "Offline fallback only — real rates come from "
            "ingestion/api_ingest/fetch_fx_rates.py."
        ),
    )
    args = parser.parse_args()

    database_url = get_database_url()
    parsed = urlparse(database_url)
    logger.info(
        "Connecting to host=%s database=%s schema=retail_oltp",
        parsed.hostname,
        parsed.path.lstrip("/"),
    )

    with psycopg.connect(database_url) as connection:
        run_seed_steps(connection)
        if args.with_demo_fx:
            seed_demo_exchange_rates(connection)
        connection.commit()
    logger.info("Seed data loaded successfully.")


if __name__ == "__main__":
    main()
