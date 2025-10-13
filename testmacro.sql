{% macro test_retention_extraction() %}
    {# /*------------------------------------------------------------*/ #}
    {# /*  Purpose: Test if retention_threshold value is extracted
            correctly from post_hook in model config. */ #}
    {# /*------------------------------------------------------------*/ #}

    {% set nodes_dict = context.get('graph', {}).get('nodes', {}) %}
    {% if not nodes_dict %}
        {{ log("⚠️ No graph context detected. Please run with 'dbt compile' or 'dbt run'.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% for node in nodes_dict.values() %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set post_hooks = node.config.get('post_hook', []) %}
            {% set expire_snapshots_hook = none %}
            {% if post_hooks is iterable and post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {% if 'expire_snapshots' in hook %}
                        {% set expire_snapshots_hook = hook %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {% if expire_snapshots_hook %}
                {# extract the retention value (e.g., '7d') from the SQL string #}
                {% set retention_value = (
                    expire_snapshots_hook
                    | regex_search("retention_threshold\\s*=>\\s*'([^']+)'", 1)
                ) %}
                {{ log("Model: " ~ model_name ~ " | Retention Value: " ~ retention_value, info=True) }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
