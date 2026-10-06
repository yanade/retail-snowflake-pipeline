-- int_fx_rates_gbp filters on the base currency; any other base would be dropped silently (ADR-012).
select *
from {{ ref('stg_exchange_rates') }}
where base_currency_code != '{{ var("fx_base_currency") }}'
