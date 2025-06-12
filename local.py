- id: refined_archer_control_objective_status
  type: io.kestra.plugin.core.log.Log
  message: >
    {% set out = outputs.refined_archer_control_objective.apiResponse.output %}
    {% if 'SUCCESS' in out %}
      Status = Success
    {% else %}
      Status = Failure
    {% endif %}
