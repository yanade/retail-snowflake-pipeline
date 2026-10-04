-- Current state of each order, one row per order_id.
with versions as (
    {{ change_log_versions(source('raw', 'orders'), 'order_id') }}
)

select * exclude (dbt_valid_from, dbt_valid_to)   -- intervals stay in the history model
from versions
where dbt_valid_to is null                        -- null = no later version exists
