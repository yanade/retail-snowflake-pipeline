-- Newest version of each order line the pipeline saw, accepted to staging or rejected to dead-letter (ADR-020).
-- Money in integer cents: DVT compares decimals as float32.
with accepted as (
    select order_item_id, quantity, unit_price, updated_at, 0 as _rank
    from ecommerce_db.dbt_dev_staging.stg_order_items
),

rejected as (
    select
        to_number(source_key) as order_item_id,
        try_to_number(payload:quantity::string) as quantity,
        try_to_number(payload:unit_price::string, 12, 2) as unit_price,  -- scale is required, else it rounds
        try_to_timestamp_ntz(payload:updated_at::string) as updated_at,
        1 as _rank                                                         -- on a tie the accepted version wins
    from (
        select source_key, parse_json(raw_payload) as payload
        from ecommerce_db.dbt_dev_staging.stg_dead_letter
        where source_table = 'order_items'
    )
)

select
    order_item_id,
    quantity,
    (unit_price * 100)::number(38,0) as unit_price_cents,                 -- integer cents, as in the source query
    (quantity * unit_price * 100)::number(38,0) as line_amount_cents
from (select * from accepted union all select * from rejected)
qualify row_number() over (partition by order_item_id order by updated_at desc, _rank) = 1
