-- One row per store or sales channel. Grain: store_id.
select
    store_id as store_key,                     -- source id: stable across rebuilds
    store_id,
    store_code,                                -- unique in the source, unlike sku
    store_name,
    store_type,                                -- ONLINE / PHYSICAL / OUTLET
    city,
    country_code,
    opened_date,
    closed_date,                               -- null while open
    is_active
from {{ ref('stg_stores') }}
