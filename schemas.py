from backend.generator_service.schema_components import USERNAME_PROPERTY

task_id = [0, 1, 2]  # TODO: temporal solution!


RUN_TASK_SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "username": USERNAME_PROPERTY,
        "task_id": {
            "type": "number",
            "enum": task_id
        }
    },

    "required": ["username", "task_id"]
}

SCENARIO_SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "type": "array",
    "items": {
        "type": "string"
    }
}

