{% macro test_retention_extraction() %}
    {# /* Access manifest in memory via adapter context */ #}

    {% set manifest = context.get('manifest') %}
    {% if not manifest %}
        {{ log("Manifest not found in runtime context.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% for node_name, node in manifest.nodes.items() %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set hooks = node.config.get('post_hook', []) %}

            {% if hooks is iterable and hooks | length > 0 %}
                {% for hook in hooks %}
                    {{ log("Model: " ~ model_name ~ " | Hook: " ~ hook, info=True) }}
                {% endfor %}
            {% else %}
                {{ log("Model: " ~ model_name ~ " | No post_hook defined", info=True) }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
