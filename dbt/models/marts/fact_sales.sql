{{ config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_strategy='merge'
) }}

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
    greatest(line_loaded_at, order_loaded_at, rate_loaded_at) as source_loaded_at,   -- newest input behind this row
    sysdate() as loaded_at                     -- NTZ in UTC whatever the session time zone
from {{ ref('int_order_lines_routed') }}
where error_reason is null
{% if is_incremental() %}
    -- a new line, a changed order (status) or a late rate since the last run
    and greatest(line_loaded_at, order_loaded_at, rate_loaded_at)
        > (select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
