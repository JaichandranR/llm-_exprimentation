{% macro audit_rcc_codes() %}
{# /*--------------------------------------------------------------
    Macro: audit_rcc_codes
    Purpose:
    1. Scan all dbt models in the project.
    2. Check if each model has `rcc_code` and `purge_date_field`
       defined in its schema.yml config.
    3. Return a queryable table of audit results.
--------------------------------------------------------------*/ #}

    {% set results = [] %}

    {# /* Loop through all models in the project graph */ #}
    {% for node in graph.nodes.values() 
        if node.resource_type == 'model' and not node.name.startswith('audit_') %}
        
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

    {# /* Convert results list to a queryable table */ #}
    {% if execute %}
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
        {{ return(query) }}
    {% endif %}
{% endmacro %}


{# /*--------------------------------------------------------------
    Model: audit_rcc_status
    Purpose:
    - Run the audit_rcc_codes macro.
    - Materialize results as a persistent table.
--------------------------------------------------------------*/ #}

{{ audit_rcc_codes() }}
