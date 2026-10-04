-- Current state of each currency, one row per currency_code.
with versions as (
    {{ change_log_versions(source('raw', 'currencies'), 'currency_code') }}
)

select * exclude (dbt_valid_from, dbt_valid_to)   -- intervals stay in the history model
from versions
where dbt_valid_to is null                        -- null = no later version exists
