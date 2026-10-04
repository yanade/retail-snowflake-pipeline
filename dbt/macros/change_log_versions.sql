
{% macro change_log_versions(relation, pk) %}
-- One row per real version of each key, with its validity interval (ADR-020 History).

with deduped as (
    select *
    from {{ relation }}
    -- same version loaded twice (e.g. full_reload): keep the first load, deterministically
    qualify row_number() over (
        partition by {{ pk }}, updated_at
        order by _loaded_at, _source_file
    ) = 1
)

select
    *,
    updated_at as dbt_valid_from,
    -- next version's start closes this one; null means current
    lead(updated_at) over (partition by {{ pk }} order by updated_at) as dbt_valid_to
from deduped

{% endmacro %}
