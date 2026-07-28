# PostgreSQL Operational Source Database

This folder contains the PostgreSQL 16 operational source database for the
retail analytics pipeline.

The database models a normalized international retail OLTP system that can feed:

```text
PostgreSQL -> Azure Data Factory -> ADLS Gen2 -> Databricks -> Snowflake -> dbt
```

## Files

```text
database/
├── README.md
├── docker-compose.yml
├── schema.sql
├── seed.py
└── generate_data.py
```

## Frozen Table List

```text
customers
customer_addresses
product_categories
suppliers
products
currencies
exchange_rates
stores
employees
orders
order_items
payments
```

This is an OLTP schema, not a star schema. The later Snowflake warehouse can
derive dimensions and facts from this normalized source.

## Incremental Loading

Every mutable source table includes:

```text
created_at
updated_at
```

PostgreSQL triggers automatically refresh `updated_at` when a row changes.
Azure Data Factory can use `updated_at` as the watermark column:

```sql
select *
from retail_oltp.orders
where updated_at > :last_watermark
order by updated_at;
```

## Intentional Data Quality Design

The schema deliberately allows realistic operational data problems:

| Scenario | Where it appears |
|---|---|
| Cancelled orders | `orders.order_status = 'CANCELLED'` |
| Duplicate business keys | `orders.order_number`, `products.sku`, `customers.email` are not unique |
| Late-arriving updates | `updated_at` changes through triggers |
| Missing customer information | nullable customer names, email, phone, and `orders.customer_id` |
| Negative quantities for returns | `order_items.quantity` allows negative values |
| Invalid payment status | `payments.payment_status` is text, not an enum/check |
| Missing exchange rates | not every order currency/date must exist in `exchange_rates` |
| Unresolved product references | nullable `product_id` plus captured `source_product_sku` |

## Start PostgreSQL

```bash
cd database
docker compose up -d
```

Connection details:

```text
host: localhost
port: 5432
database: retail_source
user: retail_user
password: retail_password
```

## Load Reference Data

`DATABASE_URL` is required — there is no default fallback, so you always
consciously choose which database you're targeting (local Docker or the
Azure Flexible Server). Set it in `.env` at the project root:

```text
DATABASE_URL=postgresql://retail_user:retail_password@localhost:5432/retail_source
```

or, to target Azure instead, the connection string from
`terraform output postgres_server_fqdn` plus the admin credentials from
Key Vault (`postgres-admin-password`). See ADR-009 for why both exist.

```bash
python seed.py
```

`seed.py` inserts stable master/reference data only — currencies, categories,
suppliers, stores, employees, and products. It does **not** seed
`exchange_rates`: that table is time-variant, not static reference data, and
is populated separately by `ingestion/api_ingest/fetch_fx_rates.py` against
the live freecurrencyapi.com API (see the root `README.md`'s "Run FX rates
ingestion" section, using its `--write-postgres` flag).

If you need fake exchange rates for offline development without an API key,
run:

```bash
python seed.py --with-demo-fx
```

This is a fallback only — it is not part of the default seed path.

## Generate Transactional Data

```bash
python generate_data.py --days 60 --orders-per-day 75 --seed 42
```

The generator inserts realistic orders, order items, payments, missing customer
cases, returns, duplicate business keys, invalid payment statuses, unresolved
product references, and late-arriving updates.
