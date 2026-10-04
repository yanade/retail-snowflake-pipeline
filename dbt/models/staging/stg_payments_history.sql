-- Every version of every payment with its validity interval [dbt_valid_from, dbt_valid_to).
{{ change_log_versions(source('raw', 'payments'), 'payment_id') }}
