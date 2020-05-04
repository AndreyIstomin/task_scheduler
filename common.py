import uuid
import json
import jsonschema
import asyncio
from multiprocessing import Array
from abc import ABC, abstractmethod
from backend.task_scheduler_service.schemas import RESPONSE_SCHEMA


__all__ = ["ResponseStatus", "ResponseObject", "array_to_uuid", "uuid_to_array", "shorten_uuid", "TaskManagerInterface",
           "EditLockManagerInterface", "TaskInterface"]


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


def shorten_uuid(_uuid: uuid.UUID):

    return str(_uuid)[0:8]


class TaskManagerInterface(ABC):

    """
    Max wait time for scenario step to start, seconds
    """
    START_TIMEOUT = 60

    class ExecutionError(Exception):
        pass

    @abstractmethod
    def run_request(self, task_uuid: uuid.UUID, routing_key: str, payload: dict):
        pass


class EditLockManagerInterface(ABC):

    @abstractmethod
    def sync(self):
        pass

    @abstractmethod
    def get_affected_cells(self, obj_type=None, obj_subtype=None) -> 'AffectedCells object':
        pass

    @abstractmethod
    def get_affected_objects(self, obj_type):
        pass


class TaskInterface(ABC):

    @abstractmethod
    def uuid(self) -> uuid.UUID:
        pass

    @abstractmethod
    def username(self) -> str:
        pass

    @abstractmethod
    def load(self, provider: 'ScenarioProvider') -> (bool, str):
        pass

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def task_manager(self) -> TaskManagerInterface:
        pass

    @abstractmethod
    def payload(self) -> dict:
        pass

    #  Async def's
    @abstractmethod
    async def run(self):
        """
        Executes asynchronously the task's scenario
        """
        pass
