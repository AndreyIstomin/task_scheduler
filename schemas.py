from LandscapeEditor.backend.schema_components import USERNAME_PROPERTY

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


RESPONSE_SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {

        "request_id": {
            "type": "string"
        },

        "status": {
            "type": "integer"
        },

        "progress": {
            "type": "number"
        },

        "message": {
            "type": "string"
        }
    },

    "required": [
        "request_id",
        "status",
        "progress",
        "message"
    ],
}


CMD_MESSAGE_SCHEMA = {
    "cmd":    {
        "type": "number"
    },

    "request_id": {
        "type": "string"
    },

    "username": USERNAME_PROPERTY,

    "required": ["cmd", "request_id", "username"]
}

SOCKET_MESSAGE_SCHEMA = CMD_MESSAGE_SCHEMA

