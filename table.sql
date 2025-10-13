{{ 
    config(
        materialized = 'table',
        on_table_exists = 'replace',
        post_hook = [
            "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')"
        ]
    ) 
}}

{# /* 
------------------------------------------------------------
Model: audit_rcc_status
Purpose:
  - Validate RCC governance coverage across all dbt models.
  - Ensure RCC codes defined in schema.yml exist in Jade catalog.
  - Compare expire_snapshot retention period against Jade purge rules.
  - Persist results for downstream auditing and dashboards.
------------------------------------------------------------
*/ #}

{# /* Execute the audit macro to collect RCC metadata */ #}
{% set audit_query %}
    {{ audit_rcc_codes(500) }}
{% endset %}

{# /* Define CTE: rcc_audit - list of models with RCC configuration */ #}
with rcc_audit as (
    {{ audit_query }}
),

{# /* Define CTE: jade_catalog - active RCC rules from Jade catalog */ #}
jade_catalog as (
    select
        classcode as jade_rcc_code,
        cast(ruleperiod as integer) as ruleperiod
    from {{ ref('88057_jade_data_retention') }}
    where lower(retentionclasscodestatus) = 'active'
),

{# /* Define CTE: validated - merge and compare RCC audit results */ #}
validated as (
    select
        a.model_name,
        a.database_name,
        a.schema_name,
        a.rcc_code,
        a.purge_date_field,
        a.retention_value,
        j.ruleperiod,
        cast(a.scan_timestamp as timestamp) as scan_timestamp,

        case
            when a.rcc_code is null then 'Missing RCC code in schema.yml'
            when j.jade_rcc_code is null then 'RCC code not found in Jade catalog'
            when a.retention_value is not null
                and regexp_extract(a.retention_value, '([0-9]+)', 1) is not null
                and cast(regexp_extract(a.retention_value, '([0-9]+)', 1) as integer) < j.ruleperiod
                then 'Snapshot retention shorter than RCC purge period'
            else 'RCC configuration valid'
        end as validation_message,

        case
            when a.rcc_code is null or j.jade_rcc_code is null then 'FAIL'
            when a.retention_value is not null
                and cast(regexp_extract(a.retention_value, '([0-9]+)', 1) as integer) < j.ruleperiod
                then 'WARN'
            else 'PASS'
        end as validation_status

    from rcc_audit a
    left join jade_catalog j
        on a.rcc_code = j.jade_rcc_code
)

{# /* Final select - ordered view of RCC validation results */ #}
select
    model_name,
    database_name,
    schema_name,
    rcc_code,
    purge_date_field,
    retention_value,
    ruleperiod,
    validation_status,
    validation_message,
    scan_timestamp
from validated
order by validation_status desc, model_name;
