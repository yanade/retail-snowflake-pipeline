-- One row per customer, plus unknown member -1 for guest checkouts (ADR-013). Grain: customer_id.
with customers as (
    select * from {{ ref('stg_customers') }}
),

last_orders as (
    select customer_id, max(order_date)::date as last_order_date
    from {{ ref('stg_orders') }}
    where customer_id is not null              -- guest orders belong to no customer
    group by customer_id
)

select
    c.customer_id as customer_key,             -- source id: stable across rebuilds
    c.customer_id,
    c.customer_number,                         -- not unique in the source by design (CRM duplicates)
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.country_code,
    c.customer_status,
    (c.email is not null or c.phone is not null) as is_contactable,   -- reported, never rejected
    c.created_at::date as first_seen_date,
    o.last_order_date as last_seen_date        -- most recent order of any status
from customers c
left join last_orders o
    on c.customer_id = o.customer_id

union all

-- the unknown member must exist as a row, or every guest sale is orphaned (ADR-013)
select
    -1        as customer_key,
    null      as customer_id,
    'UNKNOWN' as customer_number,
    null      as first_name,
    null      as last_name,
    null      as email,
    null      as phone,
    null      as country_code,
    null      as customer_status,
    false     as is_contactable,
    null      as first_seen_date,
    null      as last_seen_date
