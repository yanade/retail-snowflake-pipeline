{% test one_current_version(model, column_name) %}
-- Fails on any key whose open intervals (dbt_valid_to is null) are not exactly one.
select {{ column_name }}, count_if(dbt_valid_to is null) as open_versions
from {{ model }}
group by {{ column_name }}
having count_if(dbt_valid_to is null) != 1
{% endtest %}
