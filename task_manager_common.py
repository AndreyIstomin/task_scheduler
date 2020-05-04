import time
import uuid
import asyncio
from PluginEngine.asserts import require
from backend.task_scheduler_service import ScenarioProvider, RPCRegistry, RPCStatus, RPCData
from backend.task_scheduler_service.common import TaskManagerInterface, TaskInterface
# from backend.task_scheduler_service.rpc_common import RPCBase, RPCStatus, RPCData


TaskStatus = RPCStatus


class Task(TaskInterface):
    """
    Provide access to the task's context: state, payload, running task manager, etc.
    """
    def __init__(self, task_uuid: uuid.UUID, task_id: int, payload: dict, task_manager: TaskManagerInterface):

        self._uuid = task_uuid
        self._task_id = task_id
        self._scenario = None
        self._valid = False
        self._curr_step = None
        self._payload = payload
        self._task_manager = task_manager

    def task_manager(self):
        return self._task_manager

    def payload(self):
        return self._payload

    def uuid(self):
        return self._uuid

    def username(self):
        return self.payload.get('username', 'unknown')

    def load(self, provider: ScenarioProvider) -> (bool, str):

        self._scenario, msg = provider.get_scenario(self._task_id)

        if not self._scenario:
            self._valid = False
            return False, msg

        ok, msg = RPCRegistry.check_scenario(self._scenario)
        if not ok:
            return False, msg

        self._valid = True
        self._curr_step = 0
        return True, 'Ok'

    def current_request(self) -> str:
        require(self._valid)
        require(self._scenario)
        if self._curr_step == self._scenario.step_count():
            return None
        return self._scenario.get_request(self._curr_step)

    def has_next_step(self):
        return self._curr_step < self._scenario.step_count() - 1

    def unroll(self):
        require(self._valid)
        return True

    def close(self):
        require(self._valid)
        return True

    def name(self):
        if self._scenario:
            return self._scenario.name

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

    def status(self):
        return self._status

    def set_status(self, status: 'TaskStatus', msg=None):

        self._status = status
        self.message = msg or TaskStatus.verbose(status)


class CloseRequest:

    def __init__(self, task_uuid: uuid.UUID, task_name: str, username: str):
        self.uuid = uuid.uuid4()
        self.time = time.time()
        self.task_name = task_name
        self.task_uuid = task_uuid
        self.progress = 0.0
        self.message = 'waiting'
        self.status = RPCStatus.WAITING
        self.username = username

        self.__mock_rpc_uuid = uuid.uuid4()

    def set_in_progress(self):
        require(self.status == RPCStatus.WAITING)
        self.status = RPCStatus.IN_PROGRESS
        self.message = 'in progress'
        self.time = time.time()

    def in_progress(self):
        return self.status == RPCStatus.IN_PROGRESS

    def set_completed(self):
        self.status = RPCStatus.COMPLETED
        self.message = 'completed'
        self.progress = 1.0

    def mock_rpc(self) -> RPCData:
        return RPCData(self.__mock_rpc_uuid, 'Close request', self.progress, self.status, self.message)


class AsyncScenarioExecutor:

    def __init__(self, task: Task, queue: asyncio.Queue):
        self._task = task
        self._queue = queue
