{% if extr_suffix != none %}
    {# Handle both _dev_jules and _staging_jules #}
    {% if '_dev_jules' in new_schema_name %}
        {% set true_suffix = '_dev_jules' %}
    {% elif '_staging_jules' in new_schema_name %}
        {% set true_suffix = '_staging_jules' %}
    {% else %}
        {% set true_suffix = '_jules' %}
    {% endif %}
{% endif %}
