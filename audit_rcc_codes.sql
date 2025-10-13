{% macro audit_rcc_codes() %}
{# /*------------------------------------------------------------
    Macro: audit_rcc_codes
    Purpose:
      - Scan dbt models for RCC configuration metadata
      - Extract retention_threshold from post_hooks
      - Compare snapshot expiry vs RCC purge policy
      - Prepare tabular structure for validation and audit
-------------------------------------------------------------*/ #}

{% if execute %}

    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {% do log("⚠️ No graph context detected — running fallback mode.", info=True) %}
        {% set nodes_dict = {} %}
    {% endif %}

    {% set results = [] %}

    {% for node in nodes_dict.values() %}
        {% if node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {% set model_name = node.name %}
            {% set model_schema = node.schema %}
            {% set model_database = node.database %}
            {% set rcc_code = node.config.get('rcc_code', none) %}
            {% set purge_field = node.config.get('purge_date_field', none) %}
            {% set status = 'PASS' if rcc_code else 'FAIL' %}
            {% set message = 'RCC code defined' if rcc_code else 'Missing RCC code in schema.yml' %}

            {# --- Extract post_hook and retention_threshold --- #}
            {% set post_hooks = node.config.get('post_hook', []) %}
            {% set expire_snapshots_hook = none %}
            {% set retention_value = none %}

            {% if post_hooks is iterable and post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {% if 'expire_snapshots' in hook %}
                        {% set expire_snapshots_hook = hook %}
                        {% set match = modules.re.search("retention_threshold\\s*=>\\s*'([^']+)'", hook) %}
                        {% if match %}
                            {% set retention_value = match.group(1) %}
                        {% endif %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {{ log("📘 Model: " ~ model_name ~ 
                   " | RCC: " ~ (rcc_code if rcc_code else 'None') ~ 
                   " | Retention: " ~ (retention_value if retention_value else 'None'), info=True) }}

            {% do results.append({
                'model_name': model_name,
                'database_name': model_database,
                'schema_name': model_schema,
                'rcc_code': rcc_code,
                'purge_date_field': purge_field,
                'retention_threshold': expire_snapshots_hook,
                'retention_value': retention_value,
                'status': status,
                'message': message,
                'scan_timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}

        {% endif %}
    {% endfor %}

    {# --- Build the SQL table output --- #}
    {% if results | length == 0 %}
        {% do log("⚠️ No models found for RCC audit.", info=True) %}
        {% set query %}
        SELECT
            NULL AS model_name,
            NULL AS database_name,
            NULL AS schema_name,
            NULL AS rcc_code,
            NULL AS purge_date_field,
            NULL AS retention_threshold,
            NULL AS retention_value,
            'SKIPPED' AS status,
            'No models found for RCC audit' AS message,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS scan_timestamp
        {% endset %}
    {% else %}
        {% set columns = [
            'model_name',
            'database_name',
            'schema_name',
            'rcc_code',
            'purge_date_field',
            'retention_threshold',
            'retention_value',
            'status',
            'message',
            'scan_timestamp'
        ] %}

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
                    {% if row.retention_threshold %}'{{ row.retention_threshold }}'{% else %}NULL{% endif %},
                    {% if row.retention_value %}'{{ row.retention_value }}'{% else %}NULL{% endif %},
                    '{{ row.status }}',
                    '{{ row.message }}',
                    CAST('{{ row.scan_timestamp }}' AS TIMESTAMP)
                )
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ) AS t({{ columns | join(', ') }})
        {% endset %}
    {% endif %}

    {{ return(query) }}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
