{# /* 
------------------------------------------------------------
Model: audit_rcc_status
Purpose:
  Runs the audit_rcc_codes macro and compiles cleanly
  Thread-safe and environment-agnostic
------------------------------------------------------------
*/ #}

{{
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    )
}}

{% if execute and flags.WHICH == 'run' %}
    {% do audit_rcc_codes(50) %}
{% endif %}

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
