import uuid
import json


class ResponseStatus:

    IN_PROGRESS = 0
    COMPLETED = 1
    FAILED = 2
    TIMEOUT_ERROR = 3
    CONSUMER_NOT_FOUND_ERROR = 4


class ResponseObject:

    def __init__(self, owner: uuid.UUID, request_id: uuid.UUID, status: int, progress: float,
                 error_message=''):

        self.status = status
        self.progress = progress
        self.error_message = error_message
        self.owner = owner
        self.request_id = request_id

    def to_json(self):

        return json.dumps({
            'owner': self.owner,
            'request_id': self.request_id,
            'status': self.status,
            'progress': self.progress,
            'error_message': self.error_message})

    @classmethod
    def from_json(cls, json_data):

        d = json.loads(json_data)
        return cls(**d)