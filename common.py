import uuid
import json
import jsonschema
from multiprocessing import Array
from backend.task_scheduler_service.schemas import RESPONSE_SCHEMA


__all__ = ["ResponseStatus", "ResponseObject", "array_to_uuid", "uuid_to_array"]


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


def array_to_uuid(arr: Array):

    with arr.get_lock():
	    return uuid.UUID(bytes=bytes(arr[:]))


def uuid_to_array(arr: Array, _uuid: uuid.UUID):

    with arr.get_lock():
        arr[:] = _uuid.bytes

    return arr
