import time
import uuid
from PluginEngine.common import require
from backend.task_scheduler_service import ScenarioProvider


class Task:

    def __init__(self, task_uuid: uuid.UUID, task_id: int, payload: dict):

        self._uuid = task_uuid
        self._task_id = task_id
        self._start_time = None
        self._last_heartbit_time = None
        self._scenario = None
        self._valid = False
        self._curr_step = None
        self.payload = payload

    def uuid(self):
        return self._uuid

    def load(self, provider: ScenarioProvider) -> (bool, str):

        self._scenario, msg = provider.get_scenario(self._task_id)

        if not self._scenario:
            self._valid = False
            return False, msg

        self._valid = True
        self._curr_step = 0
        return True, 'Ok'

    def start(self):
        self._curr_step = 0
        self._last_heartbit_time = self._start_time = time.time()

    def current_request(self) -> str:
        require(self._valid)
        require(self._scenario)
        if self._curr_step == self._scenario.step_count():
            return None
        return self._scenario.get_request(self._curr_step)

    def next_step(self) -> bool:
        require(self._valid)
        require(self._scenario)
        if self._curr_step < self._scenario.step_count():
            self._curr_step += 1
            return True
        else:
            return False

    def unroll(self):
        require(self._valid)
        return True

    def close(self):
        require(self._valid)
        return True

    def update_heartbit_time(self):
        require(self._valid)
        self._last_heartbit_time = time.time()

    def heartbit_time(self):
        return self._last_heartbit_time

    def name(self):
        if self._scenario:
            return self._scenario.name()


class TaskData:

    def __init__(self):
        self.task = None
        self.requests = []