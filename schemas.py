from LandscapeEditor.backend.schema_components import USERNAME_PROPERTY

RUN_TASK_SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "username": USERNAME_PROPERTY,
        "task_id": {
            "type": "string",
            "length": 36,
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

    "count": {
        "type": "number"
    },

    "less_than": {
        "type": "number"
    },

    "username": USERNAME_PROPERTY,

    "required": ["cmd"]
}

SOCKET_MESSAGE_SCHEMA = CMD_MESSAGE_SCHEMA

