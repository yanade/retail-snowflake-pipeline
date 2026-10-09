-- Current state of each dead-letter record, one row per record_id (ADR-012).
select *
from {{ source('raw', 'dead_letter') }}
-- reprocessing only moves forward, so the reprocessed version is the newest; reloads tie-break on load time
qualify row_number() over (
    partition by record_id
    order by reprocessed_at desc nulls last, _loaded_at desc
) = 1
