import uuid
import time

from backend.task_scheduler_service import ResponseObject, ResponseStatus, ScenarioProvider


class TaskStatus:
    INACTIVE, IN_PROGRESS, COMPLETED, FAILED = [0, 1, 3, 4]


class Task:

    def __init__(self, corr_id: bytes, task_id: int):

        self._corr_id = corr_id
        self._task_id = task_id
        self._start_time = None
        self._last_heartbit_time = None
        self._scenario = None
        self._valid = False
        self._status = TaskStatus.INACTIVE

    def begin(self, provider: ScenarioProvider):

        self._scenario = provider.get_scenario(self._task_id)

        if not self._scenario:
            self._valid = False
            return False

        self._valid = True
        self._status = TaskStatus.IN_PROGRESS
        self._last_heartbit_time = self._start_time = time.time()

    def next_step(self):

        return True

    def unroll(self):

        return True

    def close(self):

        return True

    def update_heartbit_time(self):
        self._last_heartbit_time = time.time()

    def heartbit_time(self):
        return self._last_heartbit_time

    def status(self):
        return self._status

    def valid(self):
        return self._valid


class TaskManager:

    def __init__(self, scenario_provider: ScenarioProvider):

        self._tasks = {}
        self._scenario_provider = scenario_provider

    def add_task(self, task_id: int, payload: dict):

        corr_id = uuid.uuid4()
        task = Task(corr_id, task_id)
        task.begin(provider=self._scenario_provider)  # TODO ???
        if task.status() == TaskStatus.IN_PROGRESS:
            self._tasks[corr_id] = task
            return corr_id
        else:
            #  TODO: how do we need react here?
            return None

    def update_task_status(self, response: ResponseObject):

        if response.corr_id not in self._tasks:
            return

        task = self._tasks[response.corr_id]
        task.update_heartbit_time()

        if response.status == ResponseStatus.FAILED:
            task.unroll()
            del self._tasks[response.corr_id]

        elif response.status == ResponseStatus.IN_PROGRESS:
            #  Here is the place to log progress
            pass

        elif response.status == ResponseStatus.COMPLETED:
            if task.status() == TaskStatus.IN_PROGRESS:
                task.next_step()
            elif task.status() == TaskStatus.COMPLETED:
                task.close()
                del self._tasks[response.corr_id]
            else:
                raise Exception('incorrect task state')
        else:
            raise Exception('unknown response status')




