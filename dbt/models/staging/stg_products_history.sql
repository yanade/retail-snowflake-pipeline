-- Every version of every product with its validity interval [dbt_valid_from, dbt_valid_to).
{{ change_log_versions(source('raw', 'products'), 'product_id') }}
