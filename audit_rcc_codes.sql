{% macro audit_rcc_codes(batch_size=50) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Auto-create the RCC audit table if missing.
  - Scan all dbt models for RCC metadata.
  - Insert records into the table in small chunks.
  - Capture snapshot retention_threshold from post_hook.
------------------------------------------------------------
*/ #}

{% if execute %}

    {# /* Ensure the table exists before inserts */ #}
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

    {# /* Get all nodes from the graph context */ #}
    {% set nodes = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes %}
        {% do log("No dbt graph context found, exiting macro.", info=True) %}
        {% do run_query("INSERT INTO {{ this }} VALUES ('NO_MODELS', 'N/A', 'N/A', NULL, NULL, NULL, 'SKIPPED', 'No dbt models found', CURRENT_TIMESTAMP)") %}
        {% do return("SELECT * FROM {{ this }}") %}
    {% endif %}

    {# /* Collect RCC configuration for each model */ #}
    {% set all_rows = [] %}
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

    {# /* Handle case where no models are found */ #}
    {% if all_rows | length == 0 %}
        {% do log("No models found to audit.", info=True) %}
        {% do run_query("INSERT INTO {{ this }} VALUES ('NO_MODELS', 'N/A', 'N/A', NULL, NULL, NULL, 'SKIPPED', 'No dbt models found', CURRENT_TIMESTAMP)") %}
        {% do return("SELECT * FROM {{ this }}") %}
    {% endif %}

    {# /* Insert records into the table in small chunks */ #}
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

    {% do log("RCC audit completed successfully. Total models processed: " ~ (all_rows | length), info=True) %}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}
{% endmacro %}
