-- Exactly one unknown member, or guest sales in fact_sales are orphaned or doubled (ADR-013).
select count_if(customer_key = -1) as unknown_members
from {{ ref('dim_customer') }}
having count_if(customer_key = -1) != 1
