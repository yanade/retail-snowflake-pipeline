-- Every version of every order with its validity interval [dbt_valid_from, dbt_valid_to).
{{ change_log_versions(source('raw', 'orders'), 'order_id') }}
