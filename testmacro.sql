{% macro test_retention_extraction() %}
    {# /*------------------------------------------------------------*/ #}
    {# /* Test reading retention_threshold from model post_hook using graph context */ #}
    {# /* Works in dbt 1.8.0 without load_file or fromjson */ #}
    {# /*------------------------------------------------------------*/ #}

    {% set graph = context.get('graph') %}
    {% if not graph %}
        {{ log("⚠️ Graph context not found. Run this with 'dbt compile' or 'dbt run' first.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set post_hooks = node.config.get('post_hook', []) %}

            {% if post_hooks is iterable and post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {% if 'expire_snapshots' in hook %}
                        {% set retention_value = hook | regex_search("retention_threshold\\s*=>\\s*'([^']+)'", 1) %}
                        {{ log("Model: " ~ model_name ~ " | Retention Value: " ~ retention_value, info=True) }}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
