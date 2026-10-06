-- Every order line lands in exactly one of fact_sales or fact_sales_rejected: 0 = lost, 2 = counted twice.
with landed as (
    select sale_id as order_item_id from {{ ref('fact_sales') }}
    union all
    select order_item_id from {{ ref('fact_sales_rejected') }}
)

select l.order_item_id, count(x.order_item_id) as landings
from {{ ref('int_order_lines') }} l
left join landed x
    on l.order_item_id = x.order_item_id
group by l.order_item_id
having count(x.order_item_id) != 1
