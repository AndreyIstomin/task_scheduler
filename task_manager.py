import uuid
import time

from backend.task_scheduler_service import ResponseObject, ResponseStatus


class Scenario:

    pass


class ScenarioProvider:

    def __init__(self):
        pass

    def get_scenario(self, task_id):
        return Scenario()


class TaskStatus:
    inactive, in_progress, completed, failed = [0, 1, 3, 4]


class Task:

    def __init__(self, corr_id: bytes, task_id: int):

        self._corr_id = corr_id
        self._task_id = task_id
        self._start_time = None
        self._last_heartbit_time = None
        self._scenario = None
        self._valid = False
        self._status = Task.inactive

    def begin(self, provider: ScenarioProvider):

        self._scenario = provider.get_scenario(self._task_id)

        if not self._scenario:
            self._valid = False
            return False

        self._valid = True
        self._status = Task.in_progress
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


class TaskManager:

    def __init__(self):

        self._tasks = {}
        self._scenario_provider = None  # TODO

    def add_task(self, task_id: int):

        corr_id = uuid.uuid4()
        task = Task(corr_id)
        task.begin(provider=self._scenario_provider)
        if task.status() == TaskStatus.in_progress:
            self._tasks[corr_id] = task
            return corr_id
        else:
            #  TODO: how do we need react here?
            return None

    def remove_task(self, corr_id):

        del self._tasks[corr_id]

    def update_task_status(self, response: ResponseObject):

        if response.corr_id not in self._tasks:
            return

        task = self._tasks[response.corr_id]
        task.update_heartbit_time()

        if response.status == ResponseStatus.FAILED:
            task.unroll()
            self.remove_task(response.corr_id)

        elif response.status == ResponseStatus.IN_PROGRESS:
            #  Here is the place to log progress
            pass
        elif response.status == ResponseStatus.COMPLETED:
            if task.status() == TaskStatus.in_progress:
                task.next_step()
            elif task.status() == TaskStatus.completed:
                task.close()
                self.remove_task(response.corr_id)
            else:
                raise Exception('incorrect task state')
        else:
            raise Exception('unknown response status')




