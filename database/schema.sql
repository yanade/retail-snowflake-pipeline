create schema if not exists retail_oltp;

set search_path to retail_oltp;

create or replace function retail_oltp.set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

create table if not exists currencies (
    currency_code char(3) primary key,
    currency_name varchar(100) not null,
    currency_symbol varchar(10),
    decimal_places smallint not null default 2,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint chk_currencies_decimal_places check (decimal_places between 0 and 4)
);

create table if not exists product_categories (
    category_id bigserial primary key,
    parent_category_id bigint references product_categories(category_id),
    category_code varchar(50) not null,
    category_name varchar(200) not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_product_categories_category_code unique (category_code),
    constraint chk_product_categories_not_self_parent
        check (parent_category_id is null or parent_category_id <> category_id)
);

create table if not exists suppliers (
    supplier_id bigserial primary key,
    supplier_code varchar(50) not null,
    supplier_name varchar(200) not null,
    country_code char(2),
    contact_email varchar(320),
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_suppliers_supplier_code unique (supplier_code)
);

create table if not exists products (
    product_id bigserial primary key,
    sku varchar(80) not null,
    product_name varchar(255),
    category_id bigint references product_categories(category_id),
    supplier_id bigint references suppliers(supplier_id),
    brand varchar(120),
    standard_unit_price numeric(12, 2),
    default_currency_code char(3) references currencies(currency_code),
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint chk_products_standard_unit_price
        check (standard_unit_price is null or standard_unit_price >= 0)
);

comment on table products is
    'SKU is intentionally not unique to allow duplicate business-key scenarios.';

create table if not exists customers (
    customer_id bigserial primary key,
    customer_number varchar(50) not null,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(320),
    phone varchar(40),
    country_code char(2),
    customer_status varchar(30) not null default 'ACTIVE',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

comment on table customers is
    'Email and customer_number are intentionally not unique to allow CRM duplicate and guest-account scenarios.';

create table if not exists customer_addresses (
    address_id bigserial primary key,
    customer_id bigint references customers(customer_id),
    address_type varchar(30) not null default 'SHIPPING',
    address_line_1 varchar(255),
    address_line_2 varchar(255),
    city varchar(120),
    region varchar(120),
    postal_code varchar(40),
    country_code char(2),
    is_default boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists stores (
    store_id bigserial primary key,
    store_code varchar(50) not null,
    store_name varchar(200) not null,
    store_type varchar(30) not null,
    country_code char(2),
    city varchar(120),
    opened_date date,
    closed_date date,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_stores_store_code unique (store_code),
    constraint chk_stores_dates
        check (closed_date is null or opened_date is null or closed_date >= opened_date)
);

create table if not exists employees (
    employee_id bigserial primary key,
    employee_number varchar(50) not null,
    store_id bigint references stores(store_id),
    first_name varchar(100),
    last_name varchar(100),
    job_title varchar(120),
    email varchar(320),
    hire_date date,
    termination_date date,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_employees_employee_number unique (employee_number),
    constraint chk_employees_dates
        check (termination_date is null or hire_date is null or termination_date >= hire_date)
);

create table if not exists exchange_rates (
    exchange_rate_id bigserial primary key,
    rate_date date not null,
    base_currency_code char(3) not null references currencies(currency_code),
    target_currency_code char(3) not null references currencies(currency_code),
    exchange_rate numeric(18, 8) not null,
    source_system varchar(80) not null default 'demo_seed',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_exchange_rates_daily_pair
        unique (rate_date, base_currency_code, target_currency_code),
    constraint chk_exchange_rates_positive check (exchange_rate > 0),
    constraint chk_exchange_rates_currency_pair check (base_currency_code <> target_currency_code)
);

create table if not exists orders (
    order_id bigserial primary key,
    order_number varchar(50) not null,
    customer_id bigint references customers(customer_id),
    store_id bigint references stores(store_id),
    employee_id bigint references employees(employee_id),
    order_status varchar(40) not null,
    order_date timestamptz not null,
    currency_code char(3) not null references currencies(currency_code),
    subtotal_amount numeric(12, 2),
    tax_amount numeric(12, 2),
    shipping_amount numeric(12, 2),
    discount_amount numeric(12, 2),
    total_amount numeric(12, 2),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

comment on table orders is
    'Order number is intentionally not unique so duplicate order events can be detected downstream.';

create table if not exists order_items (
    order_item_id bigserial primary key,
    order_id bigint not null references orders(order_id) on delete cascade,
    line_number integer not null,
    product_id bigint references products(product_id),
    source_product_sku varchar(80),
    quantity integer not null,
    unit_price numeric(12, 2),
    discount_amount numeric(12, 2) not null default 0,
    tax_amount numeric(12, 2) not null default 0,
    line_total_amount numeric(12, 2),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint chk_order_items_line_number check (line_number > 0)
);

comment on column order_items.quantity is
    'Negative quantities are allowed because returns can be represented as operational line items.';

create table if not exists payments (
    payment_id bigserial primary key,
    order_id bigint not null references orders(order_id) on delete cascade,
    payment_reference varchar(80),
    payment_method varchar(40),
    payment_status varchar(40) not null,
    payment_amount numeric(12, 2),
    currency_code char(3) references currencies(currency_code),
    payment_date timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

comment on column payments.payment_status is
    'No check constraint by design; invalid operational statuses are validated downstream.';

create index if not exists idx_customers_updated_at on customers(updated_at);
create index if not exists idx_customers_customer_number on customers(customer_number);
create index if not exists idx_customers_email on customers(email);
create index if not exists idx_customer_addresses_updated_at on customer_addresses(updated_at);
create index if not exists idx_customer_addresses_customer_id on customer_addresses(customer_id);
create index if not exists idx_product_categories_updated_at on product_categories(updated_at);
create index if not exists idx_product_categories_parent_id on product_categories(parent_category_id);
create index if not exists idx_suppliers_updated_at on suppliers(updated_at);
create index if not exists idx_products_updated_at on products(updated_at);
create index if not exists idx_products_sku on products(sku);
create index if not exists idx_products_category_id on products(category_id);
create index if not exists idx_products_supplier_id on products(supplier_id);
create index if not exists idx_currencies_updated_at on currencies(updated_at);
create index if not exists idx_exchange_rates_updated_at on exchange_rates(updated_at);
create index if not exists idx_exchange_rates_pair_date
    on exchange_rates(base_currency_code, target_currency_code, rate_date);
create index if not exists idx_stores_updated_at on stores(updated_at);
create index if not exists idx_stores_country_code on stores(country_code);
create index if not exists idx_employees_updated_at on employees(updated_at);
create index if not exists idx_employees_store_id on employees(store_id);
create index if not exists idx_orders_updated_at on orders(updated_at);
create index if not exists idx_orders_order_date on orders(order_date);
create index if not exists idx_orders_order_number on orders(order_number);
create index if not exists idx_orders_customer_id on orders(customer_id);
create index if not exists idx_orders_store_id on orders(store_id);
create index if not exists idx_orders_currency_code on orders(currency_code);
create index if not exists idx_order_items_updated_at on order_items(updated_at);
create index if not exists idx_order_items_order_id on order_items(order_id);
create index if not exists idx_order_items_product_id on order_items(product_id);
create index if not exists idx_order_items_source_product_sku on order_items(source_product_sku);
create index if not exists idx_payments_updated_at on payments(updated_at);
create index if not exists idx_payments_order_id on payments(order_id);
create index if not exists idx_payments_payment_date on payments(payment_date);
create index if not exists idx_payments_payment_reference on payments(payment_reference);

drop trigger if exists trg_currencies_set_updated_at on currencies;
create trigger trg_currencies_set_updated_at before update on currencies
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_product_categories_set_updated_at on product_categories;
create trigger trg_product_categories_set_updated_at before update on product_categories
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_suppliers_set_updated_at on suppliers;
create trigger trg_suppliers_set_updated_at before update on suppliers
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_products_set_updated_at on products;
create trigger trg_products_set_updated_at before update on products
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_customers_set_updated_at on customers;
create trigger trg_customers_set_updated_at before update on customers
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_customer_addresses_set_updated_at on customer_addresses;
create trigger trg_customer_addresses_set_updated_at before update on customer_addresses
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_stores_set_updated_at on stores;
create trigger trg_stores_set_updated_at before update on stores
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_employees_set_updated_at on employees;
create trigger trg_employees_set_updated_at before update on employees
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_exchange_rates_set_updated_at on exchange_rates;
create trigger trg_exchange_rates_set_updated_at before update on exchange_rates
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_orders_set_updated_at on orders;
create trigger trg_orders_set_updated_at before update on orders
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_order_items_set_updated_at on order_items;
create trigger trg_order_items_set_updated_at before update on order_items
for each row execute function retail_oltp.set_updated_at();

drop trigger if exists trg_payments_set_updated_at on payments;
create trigger trg_payments_set_updated_at before update on payments
for each row execute function retail_oltp.set_updated_at();
