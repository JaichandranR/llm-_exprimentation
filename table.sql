{{
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    )
}}

{# /* Run the macro to auto-create + populate audit table */ #}
{% do audit_rcc_codes(50) %}

{# /* Return a valid query so dbt can compile the model */ #}
select
    model_name,
    schema_name,
    database_name,
    rcc_code,
    purge_date_field,
    retention_value,
    status,
    message,
    scan_timestamp
from {{ this }}
