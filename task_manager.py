import uuid
import time
from PluginEngine import Log
from PluginEngine.common import require, empty_uuid
from backend.task_scheduler_service import ResponseObject, ResponseStatus, ScenarioProvider, RPCManager, TaskLogger,\
    RPCStatus, TaskStatus, Task, TaskData, RPCErrorCallbackInterface, RPCData, CloseRequest, EditLockManager


class TaskManager:

    class ErrorCallbackHandler(RPCErrorCallbackInterface):

        def __init__(self, task_logger: TaskLogger):

            self.__logger = task_logger

        def on_response_json_decode_error(self, ex: Exception):

            self.__logger.error('Invalid JSON of RPC response')

        def on_response_json_validation_error(self, ex: Exception):

            self.__logger.error('Incorrect JSON schema of RPC response')

        def on_unknown_request_id(self):

            self.__logger.error('Unknown RPC request id')

    def __init__(self, amqp_url: str, scenario_provider: ScenarioProvider, lock_manager: EditLockManager,
                 task_logger: TaskLogger):

        self._tasks = {}
        self._requests = {}
        self._close_requests = {}
        self._closed_tasks = []
        self._scenario_provider = scenario_provider
        self._lock_manager = lock_manager
        self._task_logger = task_logger
        self._rpc_manager = None
        self._amqp_url = amqp_url

    def start_task(self, task_id: int, payload: dict) -> (uuid.UUID, str):

        """
        TODO: deal with payload
        """

        task_uuid = uuid.uuid4()
        task = Task(task_uuid, task_id, payload)
        ok, msg = task.load(provider=self._scenario_provider)  # TODO ???
        if ok:

            task.start(self._lock_manager)
            rpc = self._rpc_manager.request(task.current_request(), task.payload)

            if rpc.status == RPCStatus.WAITING:

                task_data = self._tasks[task_uuid] = TaskData()
                task_data.task = task
                task_data.requests.append(rpc)
                task_data.set_status(TaskStatus.WAITING)
                self._requests[rpc.uuid] = task.uuid()
                result = task_uuid, 'The task has been created'
            else:
                task.unroll()
                # Here the place to log put request failed
                task_data = TaskData()
                task_data.task = task
                task_data.requests = [rpc]
                task_data.set_status(TaskStatus.FAILED)
                self._closed_tasks = (task_uuid, task_data)
                #
                result = None, msg

            self._task_logger.new_task(task_data)

        else:
            self._task_logger.error(msg)
            result = None, msg

        return result

    def request_stop_task(self, task_uuid: uuid.UUID, username: str):

        if task_uuid not in self._tasks:
            return False, f'Task {task_uuid} not found'

        task_data = self._tasks[task_uuid]

        not_found = True

        for rpc in task_data.requests:

            if rpc.status in (RPCStatus.COMPLETED, RPCStatus.FAILED):
                continue

            if rpc.uuid in self._close_requests:
                continue

            ok, msg = self.start_close_request(rpc, username)
            if not ok:
                return ok, msg
            not_found = False

        return True, 'Task is already closed' if not_found else 'Task close has been requested'

    def update_task_status(self, response: ResponseObject):

        if response.request_id not in self._requests:
            msg = f'Unknown request id: {response.request_id}'
            Log.warn(msg)
            self._task_logger.warning(msg)
            return

        task_uuid = self._requests[response.request_id]
        if task_uuid not in self._tasks:
            msg = f'Unknown task id: {task_uuid}'
            Log.warn(msg)
            self._task_logger.warning(msg)
            return

        task_data = self._tasks[task_uuid]
        require(task_data.requests[-1].uuid == response.request_id)
        task = task_data.task

        rpc = task_data.requests[-1]

        rpc.message = response.message
        rpc.progress = response.progress
        rpc.update_heartbit_time()

        if response.status == ResponseStatus.FAILED:
            rpc.set_failed()

            task_data.set_status(TaskStatus.FAILED)
            task.unroll()
            self._closed_tasks = (task_uuid, task_data)
            del self._tasks[task_uuid]
            del self._requests[response.request_id]

        elif response.status == ResponseStatus.IN_PROGRESS:
            task_data.set_status(TaskStatus.IN_PROGRESS)
            rpc.progress = response.progress
            rpc.message = response.message

        elif response.status == ResponseStatus.COMPLETED:

            rpc.set_completed()

            if task.has_next_step():

                if self.is_close_requested(rpc):
                    task_data.set_status(TaskStatus.FAILED, msg=f'interrupted by {task.username()}')
                    task.unroll()
                    self._closed_tasks = (task_uuid, task_data)
                    del self._tasks[task_uuid]
                    del self._requests[response.request_id]
                else:

                    task.next_step(self._lock_manager)

                    rpc = self._rpc_manager.request(task.current_request(), task.payload)
                    if rpc.status == RPCStatus.WAITING:
                        task_data.set_status(TaskStatus.IN_PROGRESS)
                        task_data.requests.append(rpc)
                        del self._requests[response.request_id]
                        self._requests[rpc.uuid] = task_data.task.uuid()
                    else:
                        task_data.set_status(TaskStatus.FAILED)
                        task.unroll()
                        self._closed_tasks = (task_uuid, task_data)
                        del self._tasks[task_uuid]
                        del self._requests[response.request_id]

            else:
                task_data.set_status(TaskStatus.COMPLETED)
                task.close()
                self._closed_tasks = (task_uuid, task_data)
                del self._tasks[task_uuid]
                del self._requests[response.request_id]

        self.process_close_requests(rpc)
        self._task_logger.update_task(task_data)

    def start_close_request(self, rpc: RPCData, username: str):

        require(rpc.status in (RPCStatus.IN_PROGRESS, RPCStatus.WAITING))
        require(rpc.uuid in self._requests)
        task = self._tasks[self._requests[rpc.uuid]].task

        req = self._close_requests[rpc.uuid] = CloseRequest(task_uuid=self._requests[rpc.uuid], task_name=task.name(),
                                                            username=username)
        if Log.get_log_level() <= Log.TRACE:
            Log.trace('new close request')
            Log.trace(self._close_requests_info())

        # if rpc.status == RPCStatus.WAITING:
        #     ok, msg = True, f'The task is waiting'
        # else:
        #     ok, msg = self._rpc_manager.close_request(rpc.uuid, req.username)
        #     require(ok)
        #     req.set_in_progress()

        ok, msg = self._rpc_manager.close_request(rpc.uuid, req.username)
        require(ok)

        self._task_logger.update_close_request(req)

        if Log.get_log_level() <= Log.TRACE:
            Log.trace(self._close_requests_info())

        return ok, msg

    def process_close_requests(self, rpc: RPCData):

        if rpc.uuid not in self._close_requests:
            return

        req = self._close_requests[rpc.uuid]

        if rpc.status in (RPCStatus.COMPLETED, RPCStatus.FAILED):
            req.set_completed()
            self._rpc_manager.notify_task_closed(rpc.uuid, req.username)
            del self._close_requests[rpc.uuid]

        elif rpc.status == RPCStatus.IN_PROGRESS:
            if not req.in_progress():

                # ok, msg = self._rpc_manager.close_request(rpc.uuid, req.username)
                # require(ok)
                req.set_in_progress()

        self._task_logger.update_close_request(req)

        if Log.get_log_level() <= Log.TRACE:
            Log.trace(self._close_requests_info())

    def is_close_requested(self, rpc: RPCData):
        return rpc.uuid in self._close_requests

    def run_in_external_ioloop(self, io_loop):

        self._rpc_manager = RPCManager(RPCManager.CLIENT, amqp_url=self._amqp_url,
                                       heart_bit_timeout=5,
                                       reply_callback=self.update_task_status,
                                       error_callback=self.ErrorCallbackHandler(self._task_logger))

        self._rpc_manager.run_async(io_loop)

    def _close_requests_info(self) -> str:
        return '''
close requests:
------------------------------------
{requests}
------------------------------------
               '''.format(requests='\n'.join(str(req) for req in self._close_requests))



