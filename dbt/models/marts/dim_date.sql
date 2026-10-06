-- One row per calendar day from dim_date_start to dim_date_end. Grain: date_key.
with days as (
    select
        dateadd(
            day,
            row_number() over (order by seq4()) - 1,   -- seq4() may have gaps, row_number() cannot
            '{{ var("dim_date_start") }}'::date
        ) as full_date
    from table(generator(rowcount => 5000))           -- upper bound, about 13 years; trimmed below
)

select
    year(full_date) * 10000 + month(full_date) * 100 + day(full_date) as date_key,   -- YYYYMMDD
    full_date,
    year(full_date) as year,
    month(full_date) as month,
    monthname(full_date) as month_name,               -- 'Jan'
    weekiso(full_date) as week,                       -- ISO week, ignores WEEK_OF_YEAR_POLICY
    dayofweekiso(full_date) as day_of_week,           -- 1 = Monday ... 7 = Sunday, ignores WEEK_START
    dayname(full_date) as day_name,                   -- 'Mon'
    dayofweekiso(full_date) in (6, 7) as is_weekend
from days
where full_date <= '{{ var("dim_date_end") }}'::date
