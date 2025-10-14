{{
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    )
}}

{% do audit_rcc_codes(50) %}

select
    model_name,
    schema_name,
    database_name,
    rcc_code,
    purge_date_field,
    retention_value,
    validation_status,
    status,
    message,
    scan_timestamp
from {{ this }}
