{% macro audit_rcc_codes() %}
{# /*
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Scan dbt models for RCC configuration metadata
  - Extract retention_threshold from expire_snapshots post_hook
  - Prepare tabular data for auditing and validation
------------------------------------------------------------
*/ #}

{% if execute %}

    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {% do log("No graph context detected. Running fallback mode.", info=True) %}
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

            {# /* Extract expire_snapshots threshold from post_hooks */ #}
            {% set post_hooks = node.config.get('post_hook', []) %}
            {% set expire_snapshots_hook = none %}
            {% set retention_value = none %}

            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {% for hook in post_hooks %}
                {% if 'expire_snapshots' in hook %}
                    {% set expire_snapshots_hook = hook %}
                    {% set match = modules.re.search("retention_threshold\\s*=>\\s*'([^']+)'", hook) %}
                    {% if match %}
                        {% set retention_value = match.group(1) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {{ log("Model: " ~ model_name ~ 
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

    {% if results | length == 0 %}
        {% do log("No models found for RCC audit.", info=True) %}
        {{ return("SELECT NULL AS model_name, 'No models found for RCC audit' AS message") }}
    {% else %}
        {{ return(results) }}
    {% endif %}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
