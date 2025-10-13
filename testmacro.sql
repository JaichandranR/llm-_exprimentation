{% macro test_retention_extraction() %}
    {# /*------------------------------------------------------------*/ #}
    {# /* Test reading retention_threshold from post_hook via graph context */ #}
    {# /* Works in dbt 1.8.0, no load_file or fromjson required */ #}
    {# /*------------------------------------------------------------*/ #}

    {% set graph = context.get('graph') %}
    {% if not graph %}
        {{ log("Graph context not found. Run this with 'dbt compile' or 'dbt run'.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set post_hooks = node.config.get('post_hook', []) %}

            {% set retention_value = none %}

            {% if post_hooks is iterable and post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {% if 'expire_snapshots' in hook %}
                        {% set retention_value = hook | regex_search("retention_threshold\\s*=>\\s*'([^']+)'", 1) %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {{ log("Model: " ~ model_name ~ " | Retention: " ~ (retention_value if retention_value else 'None'), info=True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
