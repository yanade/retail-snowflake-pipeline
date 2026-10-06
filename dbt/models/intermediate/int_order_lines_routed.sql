-- Every order line with its GBP rate and, when it cannot be loaded, why. Grain: order_item_id.
with lines as (
    select * from {{ ref('int_order_lines') }}
),

rates as (
    select * from {{ ref('int_fx_rates_gbp') }}
)

select
    l.*,
    r.fx_rate_to_gbp,
    case
        when not l.has_order then 'missing_order'               -- order rejected upstream
        when r.fx_rate_to_gbp is null then 'missing_fx_rate'    -- never a null measure (ADR-012)
    end as error_reason                                         -- null = loadable
from lines l
left join rates r                              -- left: a line without a rate is routed, not dropped
    on l.order_date = r.rate_date
   and l.currency_code = r.currency_code
