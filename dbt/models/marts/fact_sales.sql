-- One row per order line that has an order and a GBP rate (ADR-011). Grain: sale_id.
select
    order_item_id as sale_id,                  -- derived, not generated: stable unique_key
    order_id,
    order_number,
    order_status,                              -- every revenue figure must filter on it
    coalesce(customer_id, -1) as customer_key, -- guest checkout -> unknown member (ADR-013)
    product_id as product_key,
    store_id as store_key,
    year(order_date) * 10000 + month(order_date) * 100 + day(order_date) as date_key,
    quantity,                                  -- negative = return
    currency_code,
    unit_price as unit_price_original,
    (quantity * unit_price)::number(12, 2) as total_original,                 -- gross: before discount, ex VAT
    fx_rate_to_gbp,
    round(quantity * unit_price * fx_rate_to_gbp, 2)::number(12, 2) as total_gbp,
    quantity < 0 as is_return,
    sysdate() as loaded_at                     -- NTZ in UTC whatever the session time zone
from {{ ref('int_order_lines_routed') }}
where error_reason is null
