{% macro audit_rcc_codes(batch_size=50) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes (Hybrid)
Purpose:
  - Use graph context to list all models.
  - Use node introspection to extract retention_threshold.
  - Avoid missing refined/incremental models.
------------------------------------------------------------
*/ #}

{% if execute %}

    {# /* Ensure audit table exists */ #}
    {% set create_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ this }} (
            model_name VARCHAR,
            schema_name VARCHAR,
            database_name VARCHAR,
            rcc_code VARCHAR,
            purge_date_field VARCHAR,
            retention_value VARCHAR,
            status VARCHAR,
            message VARCHAR,
            scan_timestamp TIMESTAMP
        )
    {% endset %}
    {% do run_query(create_table_sql) %}
    {% do log("Ensured audit table exists: " ~ this, info=True) %}

    {# /* Use context.graph to list all model names */ #}
    {% set graph_dict = context.get('graph', {}) %}
    {% set model_keys = graph_dict.keys() if graph_dict else [] %}
    {% set all_rows = [] %}

    {% if model_keys | length == 0 %}
        {% do log("No models found in graph context.", info=True) %}
        {% do run_query("INSERT INTO {{ this }} VALUES ('NO_MODELS','N/A','N/A',NULL,NULL,NULL,'SKIPPED','No dbt models found',CURRENT_TIMESTAMP)") %}
        {% do return("SELECT * FROM {{ this }}") %}
    {% endif %}

    {# /* Iterate using graph for full coverage */ #}
    {% for model_name in model_keys %}
        {% set node = graph.nodes.get(model_name) %}
        {% if node and node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {% set post_hooks = node.config.get('post-hook', []) %}
            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {% set retention_value = none %}
            {% for hook in post_hooks %}
                {% if 'retention_threshold' in hook %}
                    {% set part = hook.split("retention_threshold")[1] %}
                    {% set part = part.split("=>")[1] if "=>" in part else part %}
                    {% set part = part.split("'")[1] if "'" in part else part %}
                    {% set retention_value = part %}
                {% endif %}
            {% endfor %}

            {% do all_rows.append({
                'model': node.name,
                'schema': node.schema,
                'database': node.database,
                'rcc_code': node.config.get('rcc_code', none),
                'purge_field': node.config.get('purge_date_field', none),
                'retention': retention_value,
                'status': 'PASS' if node.config.get('rcc_code', none) else 'FAIL',
                'message': 'RCC code defined' if node.config.get('rcc_code', none) else 'Missing RCC code in schema.yml',
                'timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endif %}
    {% endfor %}

    {# /* Insert rows in batches */ #}
    {% for i in range(0, all_rows | length, batch_size) %}
        {% set batch = all_rows[i : i + batch_size] %}
        {% set insert_sql %}
            INSERT INTO {{ this }} (
                model_name,
                schema_name,
                database_name,
                rcc_code,
                purge_date_field,
                retention_value,
                status,
                message,
                scan_timestamp
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
        {% do run_query(insert_sql) %}
        {% do log("Inserted batch " ~ (i // batch_size + 1) ~ " with " ~ (batch | length) ~ " rows.", info=True) %}
    {% endfor %}

    {% do log("Hybrid RCC audit completed successfully. Total models processed: " ~ (all_rows | length), info=True) %}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
