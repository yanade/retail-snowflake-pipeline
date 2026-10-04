-- Current state of each order line, one row per order_item_id.
with versions as (
    {{ change_log_versions(source('raw', 'order_items'), 'order_item_id') }}
)

select * exclude (dbt_valid_from, dbt_valid_to)   -- intervals stay in the history model
from versions
where dbt_valid_to is null                        -- null = no later version exists
