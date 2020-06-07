import time
import uuid
import asyncio
import json
from typing import *
from jsonschema import validate, ValidationError
from PluginEngine.asserts import require
from PluginEngine.quadtree import QCell, make_cell_by_raw_index
from LandscapeEditor.backend import TaskInputInterface
from backend.task_scheduler_service import ScenarioProvider, RPCRegistry, RPCStatus, RPCData
from backend.task_scheduler_service.common import TaskManagerInterface, TaskInterface, LockedData, \
    EditLockManagerInterface, ObjectMap
from backend.task_scheduler_service.rpc_common import shorten_uuid


TaskStatus = RPCStatus


class TaskInput(TaskInputInterface):

    def __init__(self):
        self._data = None
        self._username = None
        self._cells = None
        self._rect = None
        self._locked_cells = None
        self._locked_objects = None

    @classmethod
    def from_dict(cls, data: dict):

        new_one = cls()
        new_one._data = data
        new_one._username = data['username']
        new_one._cells = data.get('cells', None)
        new_one._rect = data.get('rect', None)
        new_one._locked_cells = data.get('locked_cells', [])
        new_one._locked_objects = data.get('locked_objects', [])
        return new_one

    def to_dict(self):
        return self._data

    def __getitem__(self, item):
        return self._data[item]

    def cells_by_type(self, obj_type: int) -> List[QCell]:

        # d = self._locked_cells.get(obj_type, {})
        # result = []
        # for cell_indices in d.values():
        #     result.extend(map(make_cell_by_raw_index, cell_indices))
        # return result
        result = []
        for type_, subtype_, cells in self._locked_cells:
            if type_ == obj_type:
                result.extend(map(make_cell_by_raw_index, cells))

        return result

    def cells_by_subtype(self, obj_type: int, obj_subtype: int) -> List[QCell]:
        # d = self._locked_cells.get(obj_type, {})
        # cell_indices = d.get(obj_subtype, [])
        # return list(map(make_cell_by_raw_index, cell_indices))
        for type_, subtype_, cells in self._locked_cells:
            if (type_, subtype_) == (obj_type, obj_subtype):
                return list(map(make_cell_by_raw_index, cells))
        return []

    def objects_by_type(self, obj_type: int) -> List[int]:
        # d = self._locked_objects.get(obj_type, {})
        # result = []
        # for indices in d.values():
        #     result.extend(indices)
        # return result
        result = []
        for type_, subtype_, indices in self._locked_objects:
            if type_ == obj_type:
                result.extend(indices)

        return result

    def objects_by_subtype(self, obj_type: int, obj_subtype: int) -> List[int]:
        # d = self._locked_objects.get(obj_type, {})
        # return list(d.get(obj_subtype, []))
        for type_, subtype_, indices in self._locked_objects:
            if (type_, subtype_) == (obj_type, obj_subtype):
                return list(indices)
        return []

    def username(self) -> str:
        return self._username

    def rect(self) -> Union[Dict[str, float], None]:
        return self._rect

    def cells(self) -> Union[List[QCell], None]:
        return list(map(make_cell_by_raw_index, self._cells))


class InputProducer:

    def __init__(self, data: dict):
        self._data = data
        self._locked_cells: Set[LockedData] = set()
        self._locked_objects: Set[LockedData] = set()

    def make_task_input(self) -> TaskInput:
        data = dict(self._data)

        if self._locked_cells:
            d = {}
            for lock in self._locked_cells:
                self._add_cells_to_dict(lock, d)
            data['locked_cells'] = [(key[0], key[1], list(value)) for key, value in d.items()]

        if self._locked_objects:
            d = {}
            for lock in self._locked_objects:
                self._add_objects_to_dict(lock, d)
            data['locked_objects'] = [(key[0], key[1], list(value)) for key, value in d.items()]

        return TaskInput.from_dict(data)

    def add_locked_cells(self, cells: LockedData):
        self._locked_cells.add(cells)

    def remove_locked_cells(self, cells: LockedData):
        self._locked_cells.remove(cells)

    def add_locked_objects(self, objects: LockedData):
        self._locked_objects.add(objects)

    def remove_locked_objects(self, objects: LockedData):
        self._locked_objects.remove(objects)

    @staticmethod
    def _add_cells_to_dict(lock: LockedData, output: Dict[Tuple[int, int], Set[QCell]]):
        for type_id, subtypes in lock:
            for subtype_id, cells in subtypes.items():
                s = output.setdefault((type_id, subtype_id), set())
                s.update(item.get_raw_index() for item in cells)

    @staticmethod
    def _add_objects_to_dict(lock: LockedData, output: Dict[Tuple[int, int], Set[int]]):
        for type_id, subtypes in lock:
            for subtype_id, indices in subtypes.items():
                s = output.setdefault((type_id, subtype_id), set())
                s.update(indices)


class Task(TaskInterface):
    """
    Provide access to the task's context: state, payload, running task manager, etc.
    """

    def __init__(self, task_uuid: uuid.UUID, task_id: uuid.UUID, payload: Dict[str, Any], task_manager: TaskManagerInterface,
                 lock_manager: EditLockManagerInterface):

        """
        :param task_uuid: assigned to the given task by task manager
        :param task_id: UUID of this type of tasks in scenario file
        :param payload: initial payload provided by the API request
        :param task_manager: reference to the TaskManager instance
        :param lock_manager: reference to the EditLockManager instance
        """

        require(isinstance(task_id, uuid.UUID))
        self._uuid = task_uuid
        self._task_id = task_id
        self._scenario = None
        self._valid = False
        self._init_payload = payload
        self._task_manager = task_manager
        self._lock_manager = lock_manager
        self._input_producer = InputProducer(self._init_payload)

    def task_manager(self):
        return self._task_manager

    def uuid(self):
        return self._uuid

    def username(self):
        return self._init_payload['username']

    def init_payload(self) -> dict:
        return self._init_payload

    def make_task_input(self) -> TaskInputInterface:
        return self._input_producer.make_task_input()

    def load(self, provider: ScenarioProvider) -> (bool, str):

        self._scenario, msg = provider.get_scenario(self._task_id)

        if not self._scenario:
            self._valid = False
            return False, msg

        ok, msg = RPCRegistry.check_scenario(self._scenario)
        if not ok:
            return False, msg

        ok, msg = self._scenario.check_input(self._init_payload)
        if not ok:
            return False, msg

        self._valid = True
        return True, 'Ok'

    def name(self):
        if self._scenario:
            return self._scenario.name()

    def add_cells(self, cells: LockedData):
        self._input_producer.add_locked_cells(cells)

    def remove_cells(self, cells: LockedData):
        self._input_producer.remove_locked_cells(cells)

    def add_objects(self, objects: LockedData):
        self._input_producer.add_locked_objects(objects)

    def remove_objects(self, objects: LockedData):
        self._input_producer.remove_locked_objects(objects)

    # add_objects, lock_manager, remove_cells, remove_objects

    #  Async def's
    async def run(self):
        """
        Executes asynchronously the task's scenario
        """
        require(self._valid)
        await self._scenario.execute(self)


class TaskData:

    def __init__(self, task: Task):
        self.task = task
        self.requests = []
        self._status = TaskStatus.INACTIVE
        self.message = ''
        self.close_requested = False

    def status(self):
        return self._status

    def set_waiting(self):
        if self._status <= TaskStatus.WAITING:
            self._status = TaskStatus.WAITING
            self.message = 'waiting'

    def set_in_progress(self):
        if self._status <= TaskStatus.IN_PROGRESS:
            self._status = TaskStatus.IN_PROGRESS
            self.message = 'in progress'

    def set_closed(self):
        if self._status != TaskStatus.FAILED:
            self._status = TaskStatus.COMPLETED
            self.message = 'completed'

    def set_failed(self, msg=None):
        require(self._status != TaskStatus.COMPLETED)
        self._status = TaskStatus.FAILED
        self.message = msg or TaskStatus.verbose(TaskStatus.FAILED)

    def __str__(self):
        return f'task: {shorten_uuid(self.task.uuid())}: {TaskStatus.verbose(self._status)}'


class CloseRequest:

    def __init__(self, task_uuid: uuid.UUID, rpc_uuid: uuid.UUID, task_name: str, username: str, queue: asyncio.Queue):
        self.uuid = uuid.uuid4()
        self.task_name = task_name
        self.task_uuid = task_uuid
        self.rpc_uuid = rpc_uuid
        self.progress = 0.0
        self.message = 'waiting'
        self.status = RPCStatus.WAITING
        self.username = username
        self.queue = queue

        self.__mock_rpc_uuid = uuid.uuid4()

    def set_in_progress(self):
        require(self.status != RPCStatus.COMPLETED and self.status != RPCStatus.FAILED)
        self.status = RPCStatus.IN_PROGRESS
        self.message = 'in progress'

    def in_progress(self):
        return self.status == RPCStatus.IN_PROGRESS

    def set_completed(self):
        self.status = RPCStatus.COMPLETED
        self.message = 'completed'
        self.progress = 1.0

    def set_failed(self):
        self.status = RPCStatus.FAILED
        self.message = 'failed'
        self.progress = 1.0

    def set_terminate_requested(self):
        self.status = RPCStatus.IN_PROGRESS
        self.message = 'terminating'

    def mock_rpc(self) -> RPCData:
        return RPCData(self.__mock_rpc_uuid, 'Close request', self.progress, self.status, self.message)


class AsyncScenarioExecutor:

    def __init__(self, task: Task, queue: asyncio.Queue):
        self._task = task
        self._queue = queue
