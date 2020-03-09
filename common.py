import uuid
import json
import jsonschema

from backend.task_scheduler_service.schemas import RESPONSE_SCHEMA


class ResponseStatus:

    IN_PROGRESS = 0
    COMPLETED = 1
    FAILED = 2
    TIMEOUT_ERROR = 3
    CONSUMER_NOT_FOUND_ERROR = 4


class ResponseObject:

    def __init__(self, request_id: str, status: int, progress: float,
                 message=''):

        self.status = status
        self.progress = progress
        self.message = message
        self.request_id = uuid.UUID(request_id)

    def to_json(self):

        return json.dumps({
            'request_id': str(self.request_id),
            'status': self.status,
            'progress': self.progress,
            'message': self.message})

    @classmethod
    def from_json(cls, json_data: bytes):

        d = json.loads(json_data)
        jsonschema.validate(d, RESPONSE_SCHEMA)
        return cls(**d)