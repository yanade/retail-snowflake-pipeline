-- Newest version of each payment the pipeline saw, accepted to staging or rejected to dead-letter (ADR-020).
with accepted as (
    select payment_id, currency_code, payment_amount, updated_at, 0 as _rank
    from ecommerce_db.dbt_dev_staging.stg_payments
),

rejected as (
    select
        to_number(source_key) as payment_id,
        payload:currency_code::string as currency_code,
        try_to_number(payload:payment_amount::string, 12, 2) as payment_amount,  -- scale is required, else it rounds
        try_to_timestamp_ntz(payload:updated_at::string) as updated_at,
        1 as _rank                                                                 -- on a tie the accepted version wins
    from (
        select source_key, parse_json(raw_payload) as payload
        from ecommerce_db.dbt_dev_staging.stg_dead_letter
        where source_table = 'payments'
    )
)

select
    payment_id,
    currency_code,
    (payment_amount * 100)::number(38,0) as payment_amount_cents
from (select * from accepted union all select * from rejected)
qualify row_number() over (partition by payment_id order by updated_at desc, _rank) = 1
