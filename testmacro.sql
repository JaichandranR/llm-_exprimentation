{% macro test_retention_extraction() %}
    {# /* Extracts post_hook from model config directly from graph context */ #}

    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' %}
                {% set model_name = node.name %}
                {% set post_hooks = node.config.get('post_hook', []) %}

                {% if post_hooks | length > 0 %}
                    {% for hook in post_hooks %}
                        {{ log("Model: " ~ model_name ~ " | Hook: " ~ hook, info=True) }}
                    {% endfor %}
                {% else %}
                    {{ log("Model: " ~ model_name ~ " | No post_hook defined", info=True) }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("Macro running in parse-only mode, skipping graph scan.", info=True) }}
    {% endif %}
{% endmacro %}
