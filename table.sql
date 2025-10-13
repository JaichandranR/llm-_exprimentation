{{ config(
    materialized = 'table',
    on_table_exists = 'replace',
    post_hook = ["ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')"]
) }}

with rcc_audit as (
    {{ audit_rcc_codes() }}
),

jade_catalog as (
    select
        classcode as jade_rcc_code,
        cast(ruleperiod as integer) as ruleperiod
    from {{ ref('88057_jade_data_retention') }}
    where lower(retentionclasscodestatus) = 'active'
),

validated as (
    select
        a.model_name,
        a.database_name,
        a.schema_name,
        a.rcc_code,
        a.purge_date_field,
        a.retention_value,
        j.ruleperiod,
        case
            when a.rcc_code is null then 'Missing RCC code in schema.yml'
            when j.jade_rcc_code is null then 'RCC code not found in Jade catalog'
            when a.retention_value is not null
                and regexp_extract(a.retention_value, '([0-9]+)', 1) is not null
                and cast(regexp_extract(a.retention_value, '([0-9]+)', 1) as integer) < j.ruleperiod
                then 'Snapshot retention shorter than RCC purge period'
            else 'RCC configuration valid'
        end as validation_message,
        cast(a.scan_timestamp as timestamp) as scan_timestamp
    from rcc_audit a
    left join jade_catalog j
        on a.rcc_code = j.jade_rcc_code
)

select *
from validated
order by validation_message desc, model_name;
