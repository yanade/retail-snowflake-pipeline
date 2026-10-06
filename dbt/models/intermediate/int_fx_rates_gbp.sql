-- GBP per one unit of each currency, per day, via base cross rates (ADR-012). Grain: rate_date, currency_code.
with base_rates as (
    -- units of currency_code per one base unit
    select rate_date, target_currency_code as currency_code, exchange_rate
    from {{ ref('stg_exchange_rates') }}
    where base_currency_code = '{{ var("fx_base_currency") }}'
),

with_base as (
    select * from base_rates
    union all
    -- the source never stores base->base (check constraint), but it is 1 by definition
    select distinct rate_date, '{{ var("fx_base_currency") }}', 1
    from base_rates
),

reporting as (
    -- from with_base, not base_rates: when reporting = base, its row exists only here
    select rate_date, exchange_rate as base_to_reporting
    from with_base
    where currency_code = '{{ var("fx_reporting_currency") }}'
)

select
    w.rate_date,
    w.currency_code,
    (r.base_to_reporting / w.exchange_rate)::number(18, 10) as fx_rate_to_gbp
from with_base w
join reporting r                           -- inner: a day without a reporting rate has no rates at all
    on w.rate_date = r.rate_date
