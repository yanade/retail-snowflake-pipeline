-- Raw landing tables: one per served table, typed, source shape (ADR-020).
-- Change logs, not mirrors: a key appears once per exported version, so no keys or uniqueness.
-- Run as retail_dev, so the tables belong to the role that loads and reads them.

USE ROLE retail_dev;

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.currencies (
    currency_code    VARCHAR,
    currency_name    VARCHAR,
    currency_symbol  VARCHAR,
    decimal_places   NUMBER(38,0),
    is_active        BOOLEAN,
    created_at       TIMESTAMP_NTZ,
    updated_at       TIMESTAMP_NTZ,
    _source_file     VARCHAR,        -- served file the row came from
    _loaded_at       TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/currencies, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.product_categories (
    category_id         NUMBER(38,0),
    parent_category_id  NUMBER(38,0),
    category_code       VARCHAR,
    category_name       VARCHAR,
    is_active           BOOLEAN,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    _source_file        VARCHAR,        -- served file the row came from
    _loaded_at          TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/product_categories, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.suppliers (
    supplier_id    NUMBER(38,0),
    supplier_code  VARCHAR,
    supplier_name  VARCHAR,
    country_code   VARCHAR,
    contact_email  VARCHAR,
    is_active      BOOLEAN,
    created_at     TIMESTAMP_NTZ,
    updated_at     TIMESTAMP_NTZ,
    _source_file   VARCHAR,        -- served file the row came from
    _loaded_at     TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/suppliers, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.products (
    product_id             NUMBER(38,0),
    sku                    VARCHAR,
    product_name           VARCHAR,
    category_id            NUMBER(38,0),
    supplier_id            NUMBER(38,0),
    brand                  VARCHAR,
    standard_unit_price    NUMBER(12,2),
    default_currency_code  VARCHAR,
    is_active              BOOLEAN,
    created_at             TIMESTAMP_NTZ,
    updated_at             TIMESTAMP_NTZ,
    _source_file           VARCHAR,        -- served file the row came from
    _loaded_at             TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/products, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.customers (
    customer_id      NUMBER(38,0),
    customer_number  VARCHAR,
    first_name       VARCHAR,
    last_name        VARCHAR,
    email            VARCHAR,
    phone            VARCHAR,
    country_code     VARCHAR,
    customer_status  VARCHAR,
    created_at       TIMESTAMP_NTZ,
    updated_at       TIMESTAMP_NTZ,
    _source_file     VARCHAR,        -- served file the row came from
    _loaded_at       TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/customers, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.customer_addresses (
    address_id      NUMBER(38,0),
    customer_id     NUMBER(38,0),
    address_type    VARCHAR,
    address_line_1  VARCHAR,
    address_line_2  VARCHAR,
    city            VARCHAR,
    region          VARCHAR,
    postal_code     VARCHAR,
    country_code    VARCHAR,
    is_default      BOOLEAN,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    _source_file    VARCHAR,        -- served file the row came from
    _loaded_at      TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/customer_addresses, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.stores (
    store_id      NUMBER(38,0),
    store_code    VARCHAR,
    store_name    VARCHAR,
    store_type    VARCHAR,
    country_code  VARCHAR,
    city          VARCHAR,
    opened_date   DATE,
    closed_date   DATE,
    is_active     BOOLEAN,
    created_at    TIMESTAMP_NTZ,
    updated_at    TIMESTAMP_NTZ,
    _source_file  VARCHAR,        -- served file the row came from
    _loaded_at    TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/stores, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.employees (
    employee_id       NUMBER(38,0),
    employee_number   VARCHAR,
    store_id          NUMBER(38,0),
    first_name        VARCHAR,
    last_name         VARCHAR,
    job_title         VARCHAR,
    email             VARCHAR,
    hire_date         DATE,
    termination_date  DATE,
    is_active         BOOLEAN,
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    _source_file      VARCHAR,        -- served file the row came from
    _loaded_at        TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/employees, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.exchange_rates (
    exchange_rate_id      NUMBER(38,0),
    rate_date             DATE,
    base_currency_code    VARCHAR,
    target_currency_code  VARCHAR,
    exchange_rate         NUMBER(18,8),
    source_system         VARCHAR,
    created_at            TIMESTAMP_NTZ,
    updated_at            TIMESTAMP_NTZ,
    _source_file          VARCHAR,        -- served file the row came from
    _loaded_at            TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/exchange_rates, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.orders (
    order_id         NUMBER(38,0),
    order_number     VARCHAR,
    customer_id      NUMBER(38,0),
    store_id         NUMBER(38,0),
    employee_id      NUMBER(38,0),
    order_status     VARCHAR,
    order_date       TIMESTAMP_NTZ,
    currency_code    VARCHAR,
    subtotal_amount  NUMBER(12,2),
    tax_amount       NUMBER(12,2),
    shipping_amount  NUMBER(12,2),
    discount_amount  NUMBER(12,2),
    total_amount     NUMBER(12,2),
    created_at       TIMESTAMP_NTZ,
    updated_at       TIMESTAMP_NTZ,
    _source_file     VARCHAR,        -- served file the row came from
    _loaded_at       TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/orders, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.order_items (
    order_item_id       NUMBER(38,0),
    order_id            NUMBER(38,0),
    line_number         NUMBER(38,0),
    product_id          NUMBER(38,0),
    source_product_sku  VARCHAR,
    quantity            NUMBER(38,0),
    unit_price          NUMBER(12,2),
    discount_amount     NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    line_total_amount   NUMBER(12,2),
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    _source_file        VARCHAR,        -- served file the row came from
    _loaded_at          TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/order_items, one row per exported version (ADR-020)';

CREATE TABLE IF NOT EXISTS ecommerce_db.raw.payments (
    payment_id         NUMBER(38,0),
    order_id           NUMBER(38,0),
    payment_reference  VARCHAR,
    payment_method     VARCHAR,
    payment_status     VARCHAR,
    payment_amount     NUMBER(12,2),
    currency_code      VARCHAR,
    payment_date       TIMESTAMP_NTZ,
    created_at         TIMESTAMP_NTZ,
    updated_at         TIMESTAMP_NTZ,
    _source_file       VARCHAR,        -- served file the row came from
    _loaded_at         TIMESTAMP_NTZ   -- when COPY loaded it, UTC
) COMMENT = 'Change log of served/payments, one row per exported version (ADR-020)';
