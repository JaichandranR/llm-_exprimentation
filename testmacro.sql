{% macro test_retention_extraction() %}
    {# Debug macro: Reads full SQL text of one model file #}
    {% if execute %}
        {% set model_name = "89858_capture_change_incident_history" %}

        {# Find model node from graph #}
        {% set model_node = graph.nodes.values()
            | selectattr("resource_type", "equalto", "model")
            | selectattr("name", "equalto", model_name)
            | list
            | first %}

        {% if not model_node %}
            {{ log("Model not found in graph: " ~ model_name, info=True) }}
            {% do return(none) %}
        {% endif %}

        {# Get the absolute file path and content #}
        {% set file_path = model_node.original_file_path %}
        {% set file_content = load_file(file_path, True) %}

        {{ log("==== Debugging Model ====", info=True) }}
        {{ log("Model Name: " ~ model_name, info=True) }}
        {{ log("File Path: " ~ file_path, info=True) }}

        {{ log("==== File Content Start ====", info=True) }}
        {{ log(file_content, info=True) }}
        {{ log("==== File Content End ====", info=True) }}

    {% else %}
        {{ log("Macro running in parse-only mode; skipping file load.", info=True) }}
    {% endif %}
{% endmacro %}
