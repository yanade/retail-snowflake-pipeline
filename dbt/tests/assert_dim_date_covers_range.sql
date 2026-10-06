-- Fails if generator's rowcount cut the calendar short, or a day is missing or doubled.
select count(*) as day_count
from {{ ref('dim_date') }}
having count(*) != datediff(day, '{{ var("dim_date_start") }}'::date, '{{ var("dim_date_end") }}'::date) + 1
