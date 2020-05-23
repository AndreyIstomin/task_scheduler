import uuid
import json
import jsonschema
from typing import *
from multiprocessing import Array
from abc import ABC, abstractmethod
from PluginEngine import Log, quadtree
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.backend import TaskInputInterface
from backend.task_scheduler_service.schemas import RESPONSE_SCHEMA


__all__ = ["TypeList", "ObjectMap", "CellMap",
           "ResponseStatus", "ResponseObject", "array_to_uuid", "uuid_to_array", "shorten_uuid",
           "TaskManagerInterface", "LockedData", "EditLockManagerInterface", "TaskInterface"]

TypeList = NewType('TypeList', List[Tuple[int, Union[List[int], None]]])
ObjectMap = NewType('ObjectType', Dict[int, Dict[int, List[Any]]])
CellMap = NewType('CellMap', Dict[int, Dict[int, List[quadtree.QCell]]])


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


def shorten_uuid(_uuid: Union[uuid.UUID, str]):

    return str(_uuid)[0:8]


class LockedData:
    def __init__(self, objects: ObjectMap, unlock: Callable[[bool], None]):
        """
        :param objects: like {(type_1: {subtype_1_1: [obj_list], subtype_1_2: [obj_list]}, type_2: ...
        None means any subtype
        :param unlock: closure for unlocking this LockedData object
        """
        self.__uuid = uuid.uuid4()
        self.__objects = objects
        self.__unlock = unlock

        self._log_locked_objects()

    def unlock(self, result: bool):
        self.__unlock(result)
        Log.trace(f"""
unlock objects ({shorten_uuid(self.__uuid)}):
------------------------------------
            """)

    def empty(self):
        return len(self.__objects) == 0

    def __len__(self):
        return len(self.__objects)

    def __iter__(self):
        return iter(self.__objects.items())

    def __hash__(self):
        return hash(self.__uuid)

    def _log_locked_objects(self, log_level=Log.TRACE):

        if Log.get_log_level() <= log_level:
            text = '\n'.join(
                'type: {0}, subtype: {1}, {2} cells'.format(
                    LANDSCAPE_OBJECT_TYPE.verbose(key_1),
                    'all' if key_2 is None else key_2,
                    len(value)
                )
                for key_1, dict_1 in self.__objects.items() for key_2, value in dict_1.items())

            Log.log_message(log_level, log_type=Log.CONSOLE, message=f"""
lock objects ({shorten_uuid(self.__uuid)}):
------------------------------------
{text}
------------------------------------""")


class EditLockManagerInterface(ABC):

    def sync(self):
        pass

    @abstractmethod
    def get_affected_cells(self, obj_types: TypeList) -> LockedData:
        """
        Return affected cells list of the given type/subtype
        :param obj_types: list of object type/object subtype pairs
        Example [(8, (0, 1), 2, None)] - type 8 with subtypes 0, 1, type 2 with all subtypes
        :return: instance of LockedCells
        """
        pass

    @abstractmethod
    def get_affected_objects(self, obj_types: TypeList) -> LockedData:
        """
        Return affected object indices list of the given type/subtype
        :param obj_types: list of object type/object subtype pairs
        Example [(8, (0, 1), 2, None)] - type 8 with subtypes 0, 1, type 2 with all subtypes
        :return: instance of LockedObjects
        """
        pass


class TaskManagerInterface(ABC):
    """
    Max wait time for scenario step to start, seconds
    """
    START_TIMEOUT = 3600

    class ExecutionError(Exception):
        pass

    @abstractmethod
    def run_request(self, task_uuid: uuid.UUID, routing_key: str):
        pass

    @abstractmethod
    def notify_task_closed(self, task_uuid: uuid.UUID):
        pass

    @abstractmethod
    def lock_manager(self) -> EditLockManagerInterface:
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
    def init_payload(self) -> dict:
        pass

    @abstractmethod
    def make_task_input(self) -> TaskInputInterface:
        pass

    @abstractmethod
    def add_cells(self, cells: LockedData):
        pass

    @abstractmethod
    def remove_cells(self, cells: LockedData):
        pass

    @abstractmethod
    def add_objects(self, objects: LockedData):
        pass

    @abstractmethod
    def remove_objects(self, objects: LockedData):
        pass

    #  Async def's
    @abstractmethod
    async def run(self):
        """
        Executes asynchronously the task's scenario
        """
        pass
