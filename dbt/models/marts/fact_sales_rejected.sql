-- Order lines that cannot enter fact_sales, with the reason (ADR-012). Grain: order_item_id.
select
    order_item_id,
    order_id,
    error_reason,                              -- missing_order / missing_fx_rate
    order_date,
    currency_code,
    quantity,
    unit_price,
    sysdate() as rejected_at
from {{ ref('int_order_lines_routed') }}
where error_reason is not null
