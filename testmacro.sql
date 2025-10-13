{% macro test_retention_extraction() %}
    {# /* Diagnostic: Inspect manifest.json directly using Python modules */ #}
    {# /* Works for dbt 1.8.0 – no fromjson, no load_file required */ #}

    {% set json = modules.json %}
    {% set open_fn = modules.open %}

    {% set manifest_path = target.path ~ '/manifest.json' %}
    {% set file_handle = open_fn(manifest_path) %}
    {% set manifest = json.load(file_handle) %}

    {% for node_name, node in manifest['nodes'].items() %}
        {% if node['resource_type'] == 'model' %}
            {% set model_name = node['name'] %}
            {% set config = node.get('config', {}) %}
            {% set post_hooks = config.get('post_hook', []) %}

            {% if post_hooks | length > 0 %}
                {% for hook in post_hooks %}
                    {{ log("Model: " ~ model_name ~ " | Hook: " ~ hook, info=True) }}
                {% endfor %}
            {% else %}
                {{ log("Model: " ~ model_name ~ " | No post_hook defined", info=True) }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
