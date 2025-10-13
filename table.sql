{{ 
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    ) 
}}

{# /* 
------------------------------------------------------------
Model: audit_rcc_status
Purpose:
  - Create table for RCC audit results.
  - The macro `audit_rcc_codes()` will populate it in chunks.
------------------------------------------------------------
*/ #}

-- Step 1: create empty structure
select
    cast(null as varchar) as model_name,
    cast(null as varchar) as schema_name,
    cast(null as varchar) as database_name,
    cast(null as varchar) as rcc_code,
    cast(null as varchar) as purge_date_field,
    cast(null as varchar) as retention_value,
    cast(null as varchar) as status,
    cast(null as varchar) as message,
    cast(null as timestamp) as scan_timestamp
where false;

-- Step 2: after creation, macro will populate in chunks
{% do audit_rcc_codes(50) %}
