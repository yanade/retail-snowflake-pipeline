-- Every version of every customer with its validity interval [dbt_valid_from, dbt_valid_to).
{{ change_log_versions(source('raw', 'customers'), 'customer_id') }}
