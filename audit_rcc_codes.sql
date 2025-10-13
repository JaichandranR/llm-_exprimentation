{% macro audit_rcc_codes(batch_size=50) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes (Unified)
Purpose:
  - Enumerate all models using context.graph fallback.
  - Extract RCC code, purge field, and retention_threshold.
  - Insert results into audit table in batches.
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

    {# /* Get all nodes from context.graph (safe even in dbt run) */ #}
    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {{ log("⚠️ Warning: No graph context detected. Running fallback mode.", info=True) }}
        {% set nodes_dict = {} %}
    {% endif %}

    {% set all_rows = [] %}

    {# /* Iterate through every model node */ #}
    {% for node in nodes_dict.values() %}
        {% if node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {# -- Step 1: Extract post_hook safely -- #}
            {% set post_hooks = node.config.get('post-hook', []) %}
            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {# -- Step 2: Parse retention_threshold if exists -- #}
            {% set retention_value = none %}
            {% for hook in post_hooks %}
                {% if 'retention_threshold' in hook %}
                    {% set segment = hook.split("retention_threshold")[1] %}
                    {% if "=>" in segment %}
                        {% set part = segment.split("=>")[1] %}
                        {% if "'" in part %}
                            {% set retention_value = part.split("'")[1] %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {# -- Step 3: Append result row -- #}
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

    {% do log("Total models detected: " ~ (all_rows | length), info=True) %}

    {# /* Insert into audit table in batches */ #}
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

    {% do log("✅ RCC audit completed successfully. Total models processed: " ~ (all_rows | length), info=True) %}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
