-- Current state of each customer address, one row per address_id.
with versions as (
    {{ change_log_versions(source('raw', 'customer_addresses'), 'address_id') }}
)

select * exclude (dbt_valid_from, dbt_valid_to)   -- intervals stay in the history model
from versions
where dbt_valid_to is null                        -- null = no later version exists
