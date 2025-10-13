{% macro test_retention_extraction() %}
    {# /* Works in dbt 1.8.0 — prints model config + compiled SQL */ #}
    {% if execute %}
        {% set model_name = "89858_capture_change_incident_history" %}

        {# Get the model node from the in-memory graph #}
        {% set model_node = graph.nodes.values()
            | selectattr("resource_type", "equalto", "model")
            | selectattr("name", "equalto", model_name)
            | list
            | first %}

        {% if not model_node %}
            {{ log("Model not found in graph: " ~ model_name, info=True) }}
            {% do return(none) %}
        {% endif %}

        {{ log("==== Debugging Model ====", info=True) }}
        {{ log("Model Name: " ~ model_name, info=True) }}
        {{ log("Database: " ~ model_node.database ~ " | Schema: " ~ model_node.schema, info=True) }}

        {{ log("==== Model Config ====", info=True) }}
        {{ log(model_node.config, info=True) }}

        {{ log("==== Compiled SQL ====", info=True) }}
        {{ log(model_node.raw_code, info=True) }}

    {% else %}
        {{ log("Macro running in parse-only mode; skipping execution.", info=True) }}
    {% endif %}
{% endmacro %}
