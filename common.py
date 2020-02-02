import json


class ResponseStatus:

    IN_PROGRESS = 0
    COMPLETED = 1
    FAILED = 2


class ResponseObject:

    def __init__(self, status: ResponseStatus, progress: float, error_message=''):

        self.status = status
        self.progress = progress
        self.error_message = error_message

    def to_json(self):

        return json.dumps({'status': int(self.status), 'progress': self.progress, 'error_message': self.error_message})

    @classmethod
    def from_json(cls, json_data):

        d = json.loads(json_data)
        return cls(d['status'], d['progress'], d['error_message'])