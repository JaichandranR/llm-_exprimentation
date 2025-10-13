{% macro test_retention_extraction() %}
    {# /* Reads manifest.json via adapter.read_file() and parses using modules.json */ #}

    {% set json = modules.json %}
    {% set manifest_content = adapter.dispatch('read_manifest', 'dbt')() %}

    {% if not manifest_content %}
        {{ log("Manifest could not be loaded — ensure you ran `dbt compile` first.", info=True) }}
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


{% macro dbt__read_manifest() %}
    {# /* Executed by adapter.dispatch to read manifest.json */ #}
    {% set manifest_path = target.path ~ '/manifest.json' %}
    {% do return(adapter.read_file(manifest_path)) %}
{% endmacro %}
