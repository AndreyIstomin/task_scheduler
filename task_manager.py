import uuid
import time
from collections import defaultdict
from frozendict import frozendict
from PluginEngine.common import require
from backend.task_scheduler_service import ResponseObject, ResponseStatus, ScenarioProvider, RPCManager


class RPCStatus:
    INACTIVE, IN_PROGRESS, COMPLETED, FAILED = [0, 1, 3, 4]


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
        if self._curr_step < self._curr_step.step_count():
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


class RPCData:

    def __init__(self, request_id: uuid.UUID):

        self.uuid = request_id
        self.progress = 0.0
        self.status = RPCStatus.INACTIVE
        self.message = ''


class TaskData:

    def __init__(self):
        self.task = None
        self.requests = []


class TaskManager:

    def __init__(self, scenario_provider: ScenarioProvider):

        self._tasks = defaultdict(TaskData)
        self._closed_tasks = frozendict(TaskData)
        self._scenario_provider = scenario_provider
        self._rpc_manager = None
        self._ampq_url = 'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600'  # TODO: get it from config!!!

    def start_task(self, task_id: int, payload: dict) -> (uuid.UUID, str):

        """
        TODO: deal with payload
        Parameters
        ----------
        task_id
        payload

        Returns
        -------

        """

        task_uuid = uuid.uuid4()
        task = Task(task_uuid, task_id, payload)
        ok, msg = task.load(provider=self._scenario_provider)  # TODO ???
        if ok:

            task.start()
            request_id, msg = self._rpc_manager.put_request(task.current_request(), task_uuid, task.payload)
            rpc = RPCData(request_id)
            rpc.message = msg

            if request_id:
                rpc.status = RPCStatus.IN_PROGRESS
                task_data =self._tasks[task_uuid]
                task_data.task = task
                task_data.requests.append(rpc)
                return task_uuid, 'Ok'
            else:
                task.unroll()
                # Here the place to log put request failed
                rpc.status = RPCStatus.FAILED
                task_data = self._closed_tasks[task_uuid]
                task_data.task = task
                task_data.requests = [rpc]
                #
                return None, msg
        else:
            return None, msg

    def update_task_status(self, response: ResponseObject):

        require(response.owner in self._tasks)
        task_data = self._tasks[response.owner]
        require(task_data.requests[-1] == response.request_id)
        task = task_data.task
        task.update_heartbit_time()

        rpc = task_data.requests[-1]

        rpc.message = response.message
        rpc.progress = response.progress

        if response.status == ResponseStatus.FAILED:
            task.unroll()
            # Here is the place to handle failure
            rpc.status = RPCStatus.FAILED
            self._closed_tasks[response.owner] = task_data
            #
            del self._tasks[response.owner]

        elif response.status == ResponseStatus.IN_PROGRESS:
            #  Here is the place to log progress
            rpc.progress = response.progress
            rpc.message = response.message
            #

        elif response.status == ResponseStatus.COMPLETED:

            rpc.status = RPCStatus.COMPLETED

            if task.next_step():

                request_id, msg = self._rpc_manager.put_request(task.current_request(), response.owner, task.payload)

                rpc = RPCData(request_id)
                rpc.message = msg
                task_data.requests.append(rpc)
                if not request_id:
                    task.unroll()
                    # Here the place to log put request failed
                    rpc.status = RPCStatus.FAILED
                    self._closed_tasks[response.owner] = task_data
                    #
                    del self._tasks[response.owner]

            else:
                task.close()
                #  Here is the place to log task completeness
                self._closed_tasks[response.owner] = task_data
                #
                del self._tasks[response.owner]

    def run_in_external_ioloop(self, io_loop):

        self._rpc_manager = RPCManager(RPCManager.CLIENT, ampq_url=self._ampq_url,
                                       reply_callback=self.update_task_status,
                                       exit_callback=self.update_task_status,
                                       heart_bit_timeout=5)

        self._rpc_manager.run(io_loop)




