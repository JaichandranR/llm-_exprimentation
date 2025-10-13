{% macro audit_rcc_codes(chunk_size=500) %}
{# /* 
------------------------------------------------------------
Macro: audit_rcc_codes
Purpose:
  - Scan dbt models for RCC configuration metadata
  - Extract retention_threshold from expire_snapshots post_hook
  - Generate a ready-to-run SQL VALUES table for auditing
  - Split large model scans into manageable chunks for Trino
------------------------------------------------------------
*/ #}

{% if execute %}

    {# /* Retrieve all model nodes from dbt graph */ #}
    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {% do log("No graph context detected. Running fallback mode.", info=True) %}
        {% set nodes_dict = {} %}
    {% endif %}

    {# /* Initialize result list */ #}
    {% set rows = [] %}

    {# /* Iterate over all models to collect metadata */ #}
    {% for node in nodes_dict.values() %}
        {% if node.resource_type == 'model' and not node.name.startswith('audit_') %}

            {# /* Normalize post_hook field to list */ #}
            {% set post_hooks = node.config.get('post_hook', []) %}
            {% if post_hooks is string %}
                {% set post_hooks = [post_hooks] %}
            {% endif %}

            {# /* Extract retention_threshold value from expire_snapshots hook */ #}
            {% set retention_value = none %}
            {% for hook in post_hooks %}
                {% if 'expire_snapshots' in hook %}
                    {% set match = modules.re.search("retention_threshold\\s*=>\\s*'([^']+)'", hook) %}
                    {% if match %}
                        {% set retention_value = match.group(1) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {# /* Append model metadata to rows list */ #}
            {% do rows.append({
                'model_name': node.name,
                'database_name': node.database,
                'schema_name': node.schema,
                'rcc_code': node.config.get('rcc_code', none),
                'purge_date_field': node.config.get('purge_date_field', none),
                'retention_value': retention_value,
                'status': 'PASS' if node.config.get('rcc_code', none) else 'FAIL',
                'message': 'RCC code defined' if node.config.get('rcc_code', none) else 'Missing RCC code in schema.yml',
                'scan_timestamp': modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) %}
        {% endif %}
    {% endfor %}

    {# /* Handle empty model list case */ #}
    {% if rows | length == 0 %}
        {{ return("SELECT NULL AS model_name, 'No models found for RCC audit' AS message") }}
    {% endif %}

    {# /* Split results into smaller chunks to prevent Trino parser overflow */ #}
    {% set chunks = [] %}
    {% for i in range(0, rows | length, chunk_size) %}
        {% set chunk = rows[i : i + chunk_size] %}

        {# /* Build SQL VALUES block for each chunk */ #}
        {% set chunk_sql %}
        SELECT * FROM (
            VALUES
            {%- for row in chunk %}
                (
                    '{{ row.model_name }}',
                    '{{ row.database_name }}',
                    '{{ row.schema_name }}',
                    {% if row.rcc_code %}'{{ row.rcc_code }}'{% else %}NULL{% endif %},
                    {% if row.purge_date_field %}'{{ row.purge_date_field }}'{% else %}NULL{% endif %},
                    {% if row.retention_value %}'{{ row.retention_value }}'{% else %}NULL{% endif %},
                    '{{ row.status }}',
                    '{{ row.message }}',
                    CAST('{{ row.scan_timestamp }}' AS TIMESTAMP)
                ){% if not loop.last %},{% endif %}
            {%- endfor %}
        ) AS t (
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
        {% do chunks.append(chunk_sql) %}
    {% endfor %}

    {# /* Combine all chunks with UNION ALL */ #}
    {% set final_query = chunks | join(" UNION ALL\n") %}

    {# /* Log total models and chunk info */ #}
    {% set total = rows | length %}
    {% set total_chunks = chunks | length %}
    {% do log("RCC audit scanning completed. Models: " ~ total ~ ", Chunks: " ~ total_chunks, info=True) %}

    {{ return(final_query) }}

{% else %}
    {{ return("SELECT 'Macro executed in parse-only mode' AS info") }}
{% endif %}

{% endmacro %}
