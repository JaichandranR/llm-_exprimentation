{# /*--------------------------------------------------------------
    Model: audit_rcc_status
    Purpose:
      - Validate RCC governance coverage across all dbt models.
      - Ensure RCC codes defined in schema.yml exist in Jade catalog.
      - Persist results for downstream dashboard/auditing.
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
)

select
    a.model_name,
    a.database_name,
    a.schema_name,
    a.rcc_code,
    a.purge_date_field,
    a.status as rcc_config_status,
    case
        when a.rcc_code is null then '❌ Missing RCC code in schema.yml'
        when j.jade_rcc_code is null then '⚠️ RCC code not found in Jade catalog'
        else '✅ RCC validated in Jade catalog'
    end as validation_message,
    j.retention_days,
    j.rcc_description,
    cast(a.scan_timestamp as timestamp) as scan_timestamp
from rcc_audit a
left join jade_catalog j
    on a.rcc_code = j.jade_rcc_code


{% macro audit_rcc_codes() %}
{# /*--------------------------------------------------------------
    Macro: audit_rcc_codes
    Purpose:
      - Scan dbt models for RCC configuration metadata
      - Prepare tabular structure for validation
--------------------------------------------------------------*/ #}

    {% if execute %}
        {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
        {% if not nodes_dict %}
            {{ log("⚠️ Warning: No graph context detected. Running fallback mode.", info=True) }}
            {% set nodes_dict = {} %}
        {% endif %}

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
            {% set status = 'PASS' if rcc_code else 'FAIL' %}
            {% set message = 'RCC code defined' if rcc_code else 'Missing RCC code in schema.yml' %}

            {% do results.append({
                'model_name': model_name,
                'database_name': model_database,
                'schema_name': model_schema,
                'rcc_code': rcc_code,
                'purge_date_field': purge_field,
                'status': status,
                'message': message,
                'scan_timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endfor %}

        {% if results | length == 0 %}
            {% set query %}
                SELECT
                    NULL AS model_name,
                    NULL AS database_name,
                    NULL AS schema_name,
                    NULL AS rcc_code,
                    NULL AS purge_date_field,
                    'SKIPPED' AS status,
                    'No models found for audit' AS message,
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS scan_timestamp
            {% endset %}
        {% else %}
            {% set columns = ['model_name','database_name','schema_name','rcc_code','purge_date_field','status','message','scan_timestamp'] %}
            {% set query %}
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
                            '{{ row.status }}',
                            '{{ row.message }}',
                            CAST('{{ row.scan_timestamp }}' AS TIMESTAMP)
                        ){% if not loop.last %},{% endif %}
                    {% endfor %}
                ) AS t({{ columns | join(', ') }})
            {% endset %}
        {% endif %}

        {{ return(query) }}
    {% else %}
        {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
    {% endif %}
{% endmacro %}
