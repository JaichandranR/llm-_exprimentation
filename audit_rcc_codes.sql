{% macro audit_rcc_codes(batch_size=50) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Scan all dbt models for RCC metadata.
  - Insert results into audit_rcc_status table in small batches.
------------------------------------------------------------
*/ #}

{% if execute %}

    {% set nodes = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes %}
        {% do log("No dbt graph context found.", info=True) %}
        {% set nodes = {} %}
    {% endif %}

    {% set all_rows = [] %}

    {# /* Collect all models' RCC metadata */ #}
    {% for node in nodes.values() %}
        {% if node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {% set post_hooks = node.config.get('post_hook', []) %}
            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {% set retention = none %}
            {% for hook in post_hooks %}
                {% if 'expire_snapshots' in hook %}
                    {% set match = modules.re.search("retention_threshold\\s*=>\\s*'([^']+)'", hook) %}
                    {% if match %}
                        {% set retention = match.group(1) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {% do all_rows.append({
                'model': node.name,
                'schema': node.schema,
                'database': node.database,
                'rcc_code': node.config.get('rcc_code', none),
                'purge_field': node.config.get('purge_date_field', none),
                'retention': retention,
                'status': 'PASS' if node.config.get('rcc_code', none) else 'FAIL',
                'message': 'RCC code defined' if node.config.get('rcc_code', none) else 'Missing RCC code in schema.yml',
                'timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endif %}
    {% endfor %}

    {% if all_rows | length == 0 %}
        {% do log("No models found to audit.", info=True) %}
        {% do run_query("INSERT INTO {{ this }} VALUES ('NO_MODELS', 'N/A', 'N/A', NULL, NULL, NULL, 'SKIPPED', 'No models found', current_timestamp)") %}
        {% do return("SELECT * FROM {{ this }}") %}
    {% endif %}

    {# /* Iterate in batches and insert each one separately */ #}
    {% for i in range(0, all_rows | length, batch_size) %}
        {% set batch = all_rows[i : i + batch_size] %}
        {% set insert_query %}
            INSERT INTO {{ this }} (
                model_name, schema_name, database_name,
                rcc_code, purge_date_field, retention_value,
                status, message, scan_timestamp
            )
            VALUES
            {%- for row in batch %}
                (
                    '{{ row.model }}',
                    '{{ row.schema }}',
                    '{{ row.database }}',
                    {% if row.rcc_code %}'{{ row.rcc_code }}'{% else %}NULL{% endif %},
                    {% if row.purge_field %}'{{ row.purge_field }}'{% else %}NULL{% endif %},
                    {% if row.retention %}'{{ row.retention }}'{% else %}NULL{% endif %},
                    '{{ row.status }}',
                    '{{ row.message }}',
                    TIMESTAMP '{{ row.timestamp }}'
                ){% if not loop.last %},{% endif %}
            {%- endfor %}
        {% endset %}
        {% do run_query(insert_query) %}
        {% do log("Inserted batch " ~ (i // batch_size + 1) ~ " with " ~ (batch | length) ~ " rows.", info=True) %}
    {% endfor %}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
