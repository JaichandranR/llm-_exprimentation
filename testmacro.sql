{% macro test_retention_extraction() %}
    {# /*------------------------------------------------------------*/ #}
    {# /* Diagnostic: print full post_hook configs for each model */ #}
    {# /* Works for dbt 1.8.0, no regex yet */ #}
    {# /*------------------------------------------------------------*/ #}

    {% set graph = context.get('graph') %}
    {% if not graph %}
        {{ log("Graph context not found. Run with 'dbt compile' or 'dbt run'.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set post_hooks = node.config.get('post_hook', []) %}

            {{ log("Model: " ~ model_name, info=True) }}

            {% if post_hooks is iterable and post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {{ log("  ↳ post_hook: " ~ hook, info=True) }}
                {% endfor %}
            {% else %}
                {{ log("  ↳ No post_hook defined", info=True) }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
