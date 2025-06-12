- id: check_status
  type: io.kestra.plugin.core.log.Log
  message: >
    {% set out = outputs.refined_archer_control_objective.apiResponse.output %}
    {% if out.matches("(?s).*SUCCESS.*") %}
      Status = Success
    {% else %}
      Status = Failure
    {% endif %}
