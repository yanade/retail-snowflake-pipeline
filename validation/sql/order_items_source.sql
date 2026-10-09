-- order_items as the source holds them at the window end (ADR-020). Money in integer cents: DVT compares decimals as float32.
select
    order_item_id,
    quantity,
    (unit_price * 100)::bigint as unit_price_cents,
    (quantity * unit_price * 100)::bigint as line_amount_cents
from retail_oltp.order_items
where created_at <= '2026-10-03 15:25:24+00'
