-- payments as the source holds them at the window end (ADR-020). Money in integer cents: DVT compares decimals as float32.
select
    payment_id,
    currency_code,
    (payment_amount * 100)::bigint as payment_amount_cents
from retail_oltp.payments
where created_at <= {window_end}
