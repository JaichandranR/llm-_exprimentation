{% macro audit_rcc_codes(chunk_size=500) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Scan dbt models for RCC metadata and snapshot retention hooks.
  - Safely chunk output into UNION ALL queries for Trino compatibility.
------------------------------------------------------------
*/ #}

{% if execute %}

    {# /* Collect all model nodes */ #}
    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {% do log("No dbt graph context found — returning empty dataset.", info=True) %}
        {% set nodes_dict = {} %}
    {% endif %}

    {% set all_rows = [] %}

    {# /* Extract metadata from each model */ #}
    {% for node in nodes_dict.values() %}
        {% if node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {% set post_hooks = node.config.get('post_hook', []) %}
            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {% set retention_val = none %}
            {% for hook in post_hooks %}
                {% if 'expire_snapshots' in hook %}
                    {% set match = modules.re.search("retention_threshold\\s*=>\\s*'([^']+)'", hook) %}
                    {% if match %}
                        {% set retention_val = match.group(1) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {% do all_rows.append({
                'model_name': node.name,
                'database': node.database,
                'schema': node.schema,
                'rcc_code': node.config.get('rcc_code', none),
                'purge_field': node.config.get('purge_date_field', none),
                'retention_val': retention_val,
                'status': 'PASS' if node.config.get('rcc_code', none) else 'FAIL',
                'message': 'RCC code defined' if node.config.get('rcc_code', none) else 'Missing RCC code in schema.yml',
                'scan_time': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endif %}
    {% endfor %}

    {% if all_rows | length == 0 %}
        {{ return("SELECT 'NO_MODELS' AS model_name, 'No models found' AS message") }}
    {% endif %}

    {# /* Create chunks to avoid SQL overflow */ #}
    {% set chunks = [] %}
    {% for i in range(0, all_rows | length, chunk_size) %}
        {% set sub = all_rows[i : i + chunk_size] %}

        {% set part %}
        SELECT *
        FROM (
            VALUES
            {%- for row in sub %}
                (
                    '{{ row.model_name }}',
                    '{{ row.database }}',
                    '{{ row.schema }}',
                    {% if row.rcc_code %}'{{ row.rcc_code }}'{% else %}NULL{% endif %},
                    {% if row.purge_field %}'{{ row.purge_field }}'{% else %}NULL{% endif %},
                    {% if row.retention_val %}'{{ row.retention_val }}'{% else %}NULL{% endif %},
                    '{{ row.status }}',
                    '{{ row.message }}',
                    CAST('{{ row.scan_time }}' AS TIMESTAMP)
                ){% if not loop.last %},{% endif %}
            {%- endfor %}
        ) AS t(
            model_name,
            database_name,
            schema_name,
            rcc_code,
            purge_date_field,
            retention_value,
            status,
            message,
            scan_timestamp
        )
        {% endset %}
        {% do chunks.append(part.strip()) %}
    {% endfor %}

    {# /* Combine chunks safely with explicit newlines */ #}
    {% set final_query = chunks | join("\nUNION ALL\n") + "\n" %}
    {% do log("RCC audit compiled successfully — Models: " ~ (all_rows | length) ~ ", Chunks: " ~ (chunks | length), info=True) %}
    {{ return(final_query) }}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
