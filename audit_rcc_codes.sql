{% macro audit_rcc_codes(batch_size=50) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Auto-create audit table if missing
  - Loop through all model nodes
  - Extract rcc_code, purge_date_field, and retention_threshold
------------------------------------------------------------
*/ #}

{% if execute %}

    {# /* Step 1: Ensure audit table exists */ #}
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

    {# /* Step 2: Access graph context */ #}
    {% set graph_nodes = graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | list %}

    {% if not graph_nodes or graph_nodes | length == 0 %}
        {% do log("No models found in graph.", info=True) %}
        {% do run_query("INSERT INTO {{ this }} VALUES ('NO_MODELS','N/A','N/A',NULL,NULL,NULL,'SKIPPED','No dbt models found',CURRENT_TIMESTAMP)") %}
        {% do return("SELECT * FROM {{ this }}") %}
    {% endif %}

    {# /* Step 3: Prepare results container */ #}
    {% set all_rows = [] %}

    {# /* Step 4: Iterate through each model */ #}
    {% for node in graph_nodes %}
        {% if not node.name.startswith('audit_') %}
            
            {% set model_name = node.name %}
            {% set schema_name = node.schema %}
            {% set database_name = node.database %}
            {% set rcc_code = node.config.get('rcc_code', none) %}
            {% set purge_field = node.config.get('purge_date_field', none) %}

            {# /* Step 5: Extract post_hook safely */ #}
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

            {% set status = 'PASS' if rcc_code else 'FAIL' %}
            {% set message = 'RCC code defined' if rcc_code else 'Missing RCC code in schema.yml' %}
            {% set timestamp = modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") %}

            {% do all_rows.append({
                'model': model_name,
                'schema': schema_name,
                'database': database_name,
                'rcc_code': rcc_code,
                'purge_field': purge_field,
                'retention': retention_value,
                'status': status,
                'message': message,
                'timestamp': timestamp
            }) %}
        {% endif %}
    {% endfor %}

    {# /* Step 6: Batch insert */ #}
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


{# /* Table materialization to execute macro */ #}
{{
    config(
        materialized = 'table',
        on_table_exists = 'replace'
    )
}}

{% do audit_rcc_codes(50) %}

select
    model_name,
    schema_name,
    database_name,
    rcc_code,
    purge_date_field,
    retention_value,
    status,
    message,
    scan_timestamp
from {{ this }}
