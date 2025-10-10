{% macro audit_rcc_codes() %}
{# /*--------------------------------------------------------------
    Macro: audit_rcc_codes
    Purpose:
      - Scan dbt models for RCC configuration metadata.
      - Extract snapshot expiration thresholds from post_hooks.
      - Prepare a tabular structure for validation and audit.
--------------------------------------------------------------*/ #}

    {% if execute %}

        {# /*--- Step 1: Retrieve graph nodes ---*/ #}
        {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
        {% if not nodes_dict %}
            {{ log("⚠️ Warning: No graph context detected — fallback mode active.", info=True) }}
            {% set nodes_dict = {} %}
        {% endif %}

        {# /*--- Step 2: Iterate through each dbt model node ---*/ #}
        {% set results = [] %}
        {% for node in nodes_dict.values()
            if node.resource_type == 'model'
            and not node.name.startswith('audit_')
        %}
            {% set model_name = node.name %}
            {% set model_schema = node.schema %}
            {% set model_database = node.database %}
            {% set rcc_code = node.config.get('rcc_code', none) %}
            {% set purge_field = node.config.get('purge_date_field', none) %}
            {% set post_hooks = node.config.get('post_hook', []) %}

            {# /*--- Step 3: Detect snapshot expiry post_hook and extract retention threshold ---*/ #}
            {% set snapshot_threshold = none %}
            {% for hook in post_hooks %}
                {% if 'expire_snapshots' in hook %}
                    {% set pattern = "retention_threshold\\s*=>\\s*'([^']+)'" %}
                    {% set match = modules.re.search(pattern, hook) %}
                    {% if match %}
                        {% set snapshot_threshold = match.group(1) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {# /*--- Step 4: Add audit record ---*/ #}
            {% do results.append({
                'model_name': model_name,
                'database_name': model_database,
                'schema_name': model_schema,
                'rcc_code': rcc_code,
                'purge_date_field': purge_field,
                'snapshot_threshold': snapshot_threshold,
                'scan_timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endfor %}

        {# /*--- Step 5: Handle empty result set ---*/ #}
        {% if results | length == 0 %}
            {% set query %}
                SELECT
                    NULL AS model_name,
                    NULL AS database_name,
                    NULL AS schema_name,
                    NULL AS rcc_code,
                    NULL AS purge_date_field,
                    NULL AS snapshot_threshold,
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS scan_timestamp
            {% endset %}

        {% else %}

            {# /*--- Step 6: Define final output columns ---*/ #}
            {% set columns = [
                'model_name',
                'database_name',
                'schema_name',
                'rcc_code',
                'purge_date_field',
                'snapshot_threshold',
                'scan_timestamp'
            ] %}

            {# /*--- Step 7: Build VALUES-based SQL with explicit column exposure ---*/ #}
            {% set query %}
                WITH raw AS (
                    SELECT *
                    FROM (
                        VALUES
                        {% for row in results %}
                            (
                                '{{ row.model_name }}',
                                '{{ row.database_name }}',
                                '{{ row.schema_name }}',
                                {% if row.rcc_code %}'{{ row.rcc_code }}'{% else %}NULL{% endif %},
                                {% if row.purge_date_field %}'{{ row.purge_date_field }}'{% else %}NULL{% endif %},
                                {% if row.snapshot_threshold %}'{{ row.snapshot_threshold }}'{% else %}NULL{% endif %},
                                CAST('{{ row.scan_timestamp }}' AS TIMESTAMP)
                            ){% if not loop.last %},{% endif %}
                        {% endfor %}
                    ) AS t({{ columns | join(', ') }})
                )
                SELECT
                    model_name,
                    database_name,
                    schema_name,
                    rcc_code,
                    purge_date_field,
                    snapshot_threshold,
                    scan_timestamp
                FROM raw
            {% endset %}

        {% endif %}

        {{ log("✅ RCC Audit macro executed. Models scanned: " ~ results | length, info=True) }}
        {{ return(query) }}

    {% else %}
        {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
    {% endif %}

{% endmacro %}



{# /*--------------------------------------------------------------
    Model: audit_rcc_status
    Purpose:
      - Validate RCC and snapshot governance
      - Ensure RCC purge > snapshot retention threshold
--------------------------------------------------------------*/ #}

{% set audit_query %}
    {{ audit_rcc_codes() }}
{% endset %}

with rcc_audit as (
    {{ audit_query }}
),

jade_catalog as (
    select
        rcc_code as jade_rcc_code,
        retention_days,
        rcc_description
    from {{ ref('88057_jade_data_retention') }}
),

parsed_audit as (
    select
        a.*,
        case
            when a.snapshot_threshold like '%d' then cast(replace(a.snapshot_threshold, 'd', '') as integer)
            when a.snapshot_threshold like '%day%' then cast(regexp_replace(a.snapshot_threshold, '\\D', '') as integer)
            else null
        end as snapshot_retention_days
    from rcc_audit a
)

select
    p.model_name,
    p.database_name,
    p.schema_name,
    p.rcc_code,
    p.purge_date_field,
    j.retention_days as rcc_retention_days,
    p.snapshot_retention_days,
    case
        when p.rcc_code is null then '❌ Missing RCC code in schema.yml'
        when j.jade_rcc_code is null then '⚠️ RCC code not found in Jade catalog'
        when p.snapshot_retention_days is not null and j.retention_days > p.snapshot_retention_days
            then '⚠️ Snapshot retention (' || p.snapshot_retention_days || 'd) < RCC retention (' || j.retention_days || 'd)'
        else '✅ All retention and RCC validations passed'
    end as validation_message,
    cast(p.scan_timestamp as timestamp) as scan_timestamp
from parsed_audit p
left join jade_catalog j
    on p.rcc_code = j.jade_rcc_code

