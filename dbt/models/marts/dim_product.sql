-- One row per product, category and supplier flattened in (ADR-013). Grain: product_id.
with products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_product_categories') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
)

select
    p.product_id as product_key,               -- source id: stable across rebuilds, unlike a sequence
    p.product_id,
    p.sku,                                     -- not unique in the source (SKU-DUP-001)
    p.product_name,
    p.brand,
    c.category_name,
    s.supplier_name,
    p.standard_unit_price,                     -- list price; the price actually charged is on the fact
    p.default_currency_code,
    p.is_active,
    p.created_at::date as first_seen_date
from products p
left join categories c                         -- left: a product without a category must not vanish
    on p.category_id = c.category_id
left join suppliers s
    on p.supplier_id = s.supplier_id
