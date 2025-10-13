{% macro test_retention_extraction() %}
    {# /* Reads target/manifest.json safely and lists post_hooks */ #}

    {% set json = modules.json %}
    {% set builtins = modules.builtins %}
    {% set os = modules.os %}

    {% set manifest_path = target.path ~ '/manifest.json' %}
    {% set file_handle = builtins.open(manifest_path) %}
    {% set manifest_content = file_handle.read() %}
    {% do file_handle.close() %}

    {% if not manifest_content %}
        {{ log("Manifest not found or empty. Run `dbt compile` first.", info=True) }}
        {% do return(none) %}
    {% endif %}

    {% set manifest = json.loads(manifest_content) %}

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
