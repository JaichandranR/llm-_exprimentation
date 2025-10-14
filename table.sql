{# /* 
------------------------------------------------------------
Model: audit_rcc_status
Purpose:
  - Runs the audit_rcc_codes macro to:
      • Auto-create audit table (if missing)
      • Scan all dbt models for RCC + retention metadata
      • Compare with Jade master table
  - Returns the final audit results including validation_status
------------------------------------------------------------
*/ #}

{{
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    )
}}

{# /* Step 1: Run macro to populate audit table */ #}
{% do audit_rcc_codes(50) %}

{# /* Step 2: Return data for dbt model compilation */ #}
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
