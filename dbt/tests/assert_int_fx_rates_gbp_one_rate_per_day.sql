-- Grain: two rates for one currency on one day would fan out every order line joined to them.
select rate_date, currency_code, count(*) as rate_count
from {{ ref('int_fx_rates_gbp') }}
group by rate_date, currency_code
having count(*) > 1
