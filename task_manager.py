import uuid
import asyncio
from collections import namedtuple
from PluginEngine import Log
from PluginEngine.asserts import require
from LandscapeEditor.backend import TaskInputInterface
from backend.task_scheduler_service import ResponseObject, ResponseStatus, ScenarioProvider, RPCManager, TaskLogger,\
    RPCStatus, Task, TaskData, RPCErrorCallbackInterface, RPCData, CloseRequest
from backend.task_scheduler_service.common import TaskManagerInterface, EditLockManagerInterface


RequestData = namedtuple('RequestData', 'task_uuid, queue')


class TaskManager(TaskManagerInterface):

    class ErrorCallbackHandler(RPCErrorCallbackInterface):

        def __init__(self, task_logger: TaskLogger):

            self.__logger = task_logger

        def on_response_json_decode_error(self, ex: Exception):

            self.__logger.error('Invalid JSON of RPC response')

        def on_response_json_validation_error(self, ex: Exception):

            self.__logger.error('Incorrect JSON schema of RPC response')

        def on_unknown_request_id(self):

            self.__logger.error('Unknown RPC request id')

    def __init__(self, amqp_url: str, scenario_provider: ScenarioProvider, lock_manager: EditLockManagerInterface,
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

    async def run_request(self, task_uuid: uuid.UUID, routing_key: str):

        require(task_uuid in self._tasks, f'Unknown task id: {task_uuid}')
        task_data = self._tasks[task_uuid]
        if task_data.close_requested:
            return False

        task_input = task_data.task.make_task_input()
        rpc = self._rpc_manager.request(routing_key, task_input)
        if rpc.status == RPCStatus.WAITING:
            task_data.set_waiting()
            task_data.requests.append(rpc)
            queue = asyncio.Queue()
            self._requests[rpc.uuid] = RequestData(task_data.task.uuid(), queue)
        else:
            return False

        task_started = False
        timeout = TaskManager.START_TIMEOUT  # TODO

        self._task_logger.new_task(task_data)

        while True:

            try:

                rsp = await asyncio.wait_for(queue.get(), timeout)
                rpc.message = rsp.message
                rpc.progress = rsp.progress

                if not task_started:
                    task_started = True
                    timeout = self._rpc_manager.heartbit_timeout(routing_key)

                if rsp.status == ResponseStatus.IN_PROGRESS:
                    rpc.set_in_progress()
                    task_data.set_in_progress()

                elif rsp.status == ResponseStatus.FAILED:
                    rpc.set_failed()
                    task_data.set_failed()
                    self.request_stop_task(task_uuid, task_input.username())
                    Log.error(f'{routing_key} failed: {rsp.message}')
                    return False

                elif rsp.status == ResponseStatus.COMPLETED:
                    rpc.set_completed()
                    return True
                else:
                    Log.warn(f'Unexpected rpc response status: {rsp.status}')
                    continue

            except asyncio.TimeoutError as err:
                # The place for request's thread termination
                rpc.set_failed()
                rpc.message = f'heartbit timeout {timeout} seconds has been reached'
                task_data.set_failed()
                self.request_stop_task(task_uuid, task_input.username())
                continue

            finally:
                self.process_close_requests(rpc)
                self._task_logger.update_task(task_data)

    def notify_task_closed(self, task_uuid: uuid.UUID):

        to_delete = [key for key, value in self._requests.items() if value.task_uuid == task_uuid]
        for key in to_delete:
            del self._requests[key]

        if task_uuid in self._tasks:

            task_data = self._tasks[task_uuid]
            task_data.set_closed()
            self._task_logger.update_task(task_data)
            self._closed_tasks = task_data
            del self._tasks[task_uuid]

        self._log_task_info()

    async def start_task(self, task_id: uuid.UUID, payload: dict):

        task_uuid = uuid.uuid4()
        task = Task(task_uuid, task_id, payload, task_manager=self, lock_manager=self._lock_manager)
        ok, msg = task.load(provider=self._scenario_provider)  # TODO ???
        if ok:
            self._tasks[task_uuid] = TaskData(task)
            asyncio.create_task(task.run())

        else:
            self._task_logger.error(msg)

        self._log_task_info()

        return ok, msg

    def request_stop_task(self, task_uuid: uuid.UUID, username: str):

        if task_uuid not in self._tasks:
            return False, f'Task {task_uuid} not found'

        task_data = self._tasks[task_uuid]
        task_data.close_requested = True

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

        req_data = self._requests[response.request_id]
        if req_data.task_uuid not in self._tasks:
            msg = f'Unknown task id: {req_data.task_uuid}'
            Log.warn(msg)
            self._task_logger.warning(msg)
            return

        req_data.queue.put_nowait(response)

    def start_close_request(self, rpc: RPCData, username: str):

        require(rpc.status in (RPCStatus.IN_PROGRESS, RPCStatus.WAITING))
        require(rpc.uuid in self._requests)
        task = self._tasks[self._requests[rpc.uuid].task_uuid].task

        req = self._close_requests[rpc.uuid] = CloseRequest(task_uuid=self._requests[rpc.uuid].task_uuid,
                                                            task_name=task.name(),
                                                            username=username)
        Log.trace('new close request')
        self._log_close_requests_info()

        ok, msg = self._rpc_manager.close_request(rpc.uuid, req.username)
        require(ok)

        self._task_logger.update_close_request(req)

        self._log_close_requests_info()
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

        self._log_close_requests_info()

    def is_close_requested(self, rpc: RPCData):
        return rpc.uuid in self._close_requests

    def run_in_external_ioloop(self, io_loop):

        self._rpc_manager = RPCManager(RPCManager.CLIENT, amqp_url=self._amqp_url,
                                       reply_callback=self.update_task_status,
                                       error_callback=self.ErrorCallbackHandler(self._task_logger))

        self._rpc_manager.run_async(io_loop)

    def lock_manager(self) -> EditLockManagerInterface:
        return self._lock_manager

    def _log_close_requests_info(self, log_level=Log.TRACE):
         Log.log_message(log_level, log_type=Log.CONSOLE, message='''
close requests:
------------------------------------
{requests}
------------------------------------
               '''.format(requests='\n'.join(str(req) for req in self._close_requests)))

    def _log_task_info(self, log_level=Log.TRACE):

        Log.log_message(log_level, log_type=Log.CONSOLE, message='''
active tasks
------------------------------------
{tasks}
------------------------------------
        '''.format(tasks='\n'.join(str(task_data) for task_data in self._tasks.values())))



