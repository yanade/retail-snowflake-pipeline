-- One row per order line with its order's attributes. Grain: order_item_id.
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    i.order_item_id,
    i.order_id,
    i.line_number,
    i.product_id,
    i.quantity,                            -- negative = return
    i.unit_price,
    i.discount_amount,
    i.tax_amount,
    i.line_total_amount,
    o.order_number,
    o.customer_id,                         -- null with has_order = guest checkout (ADR-013)
    o.store_id,
    o.order_status,
    o.currency_code,
    o.order_date::date as order_date,      -- FX rates and dim_date are daily
    o.order_id is not null as has_order,   -- false = order rejected upstream, line is orphaned
    i._loaded_at as line_loaded_at,        -- both feed the incremental filter in step 10
    o._loaded_at as order_loaded_at
from order_items i
left join orders o                         -- left: an orphan line must stay visible, not vanish
    on i.order_id = o.order_id
