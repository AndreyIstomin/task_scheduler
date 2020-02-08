import json


class ResponseStatus:

    IN_PROGRESS = 0
    COMPLETED = 1
    FAILED = 2


class ResponseObject:

    def __init__(self, corr_id: bytes, status: ResponseStatus, progress: float, error_message=''):

        self.status = status
        self.progress = progress
        self.error_message = error_message
        self.corr_id = corr_id

    def to_json(self):

        return json.dumps({'corr_id': self.corr_id,
                           'status': int(self.status), 'progress': self.progress, 'error_message': self.error_message})

    @classmethod
    def from_json(cls, json_data):

        d = json.loads(json_data)
        return cls(d['corr_id'], d['status'], d['progress'], d['error_message'])