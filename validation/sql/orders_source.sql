-- orders as the source holds them at the window end (ADR-020). Money in integer cents: DVT compares decimals as float32.
select
    order_id,
    currency_code,
    (total_amount * 100)::bigint as total_amount_cents
from retail_oltp.orders
where created_at <= '2026-10-03 15:25:24+00'
