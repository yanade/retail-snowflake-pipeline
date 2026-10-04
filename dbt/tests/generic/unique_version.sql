{% test unique_version(model, column_name) %}
-- Fails on any key with two rows for the same dbt_valid_from: a version survived dedupe twice.
select {{ column_name }}, dbt_valid_from, count(*) as row_count
from {{ model }}
group by {{ column_name }}, dbt_valid_from
having count(*) > 1
{% endtest %}