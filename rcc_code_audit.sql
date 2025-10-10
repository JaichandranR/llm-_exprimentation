{% macro audit_rcc_codes() %}
{# /*--------------------------------------------------------------
    Macro: audit_rcc_codes
    Purpose:
    1. Scan all dbt models.
    2. Check if each has RCC code defined.
    3. Return tabular results for persistence.
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
                'scan_timestamp': modules.datetime.datetime.now().isoformat()
            }) %}
        {% endfor %}

        {% if results | length == 0 %}
            {{ log("⚠️ No model metadata available for audit scan.", info=True) }}
            {% set query %}
                SELECT
                    NULL AS model_name,
                    NULL AS database_name,
                    NULL AS schema_name,
                    NULL AS rcc_code,
                    NULL AS purge_date_field,
                    'SKIPPED' AS status,
                    'Graph context unavailable during model execution' AS message,
                    CURRENT_TIMESTAMP AS scan_timestamp
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
                            TIMESTAMP '{{ row.scan_timestamp }}'
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
