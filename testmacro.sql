{% macro test_retention_extraction() %}
    {# /*------------------------------------------------------------*/ #}
    {# /* Test reading retention_threshold from model post_hook using manifest.json */ #}
    {# /*------------------------------------------------------------*/ #}

    {% set manifest_path = target.path ~ '/manifest.json' %}
    {% set manifest_raw = load_file(manifest_path) %}
    {% set manifest = fromjson_string(manifest_raw) %}
    {% set nodes = manifest.nodes.values() %}

    {% for node in nodes %}
        {% if node.resource_type == 'model' %}
            {% set model_name = node.name %}
            {% set post_hooks = node.config.post_hook if node.config is defined else [] %}

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
