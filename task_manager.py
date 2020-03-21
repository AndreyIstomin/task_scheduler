import uuid
from PluginEngine import Log
from PluginEngine.common import require
from backend.task_scheduler_service import ResponseObject, ResponseStatus, ScenarioProvider, RPCManager, TaskLogger,\
    RPCStatus, Task, TaskData, RPCErrorCallbackInterface


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

    def __init__(self, ampq_url: str, scenario_provider: ScenarioProvider, task_logger: TaskLogger):

        self._tasks = {}
        self._requests = {}
        self._closed_tasks = []
        self._scenario_provider = scenario_provider
        self._task_logger = task_logger
        self._rpc_manager = None
        self._ampq_url = ampq_url

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
            rpc = self._rpc_manager.put_request(task.current_request(), task.payload)

            if rpc.status == RPCStatus.WAITING:

                task_data = self._tasks[task_uuid] = TaskData()
                task_data.task = task
                task_data.requests.append(rpc)
                self._requests[rpc.uuid] = task.uuid()
                result = task_uuid, 'The task has been created'
            else:
                task.unroll()
                # Here the place to log put request failed
                task_data = TaskData()
                task_data.task = task
                task_data.requests = [rpc]

                self._closed_tasks = (task_uuid, task_data)
                #
                result = None, msg

            self._task_logger.new_task(task_data)

        else:
            self._task_logger.error(msg)
            result = None, msg

        return result

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
            task.unroll()
            # Here is the place to handle failure
            rpc.set_failed()
            self._closed_tasks = (task_uuid, task_data)
            #
            del self._tasks[task_uuid]
            del self._requests[response.request_id]

            self._rpc_manager.close_request(rpc.uuid)

        elif response.status == ResponseStatus.IN_PROGRESS:
            #  Here is the place to log progress
            rpc.progress = response.progress
            rpc.message = response.message
            #

        elif response.status == ResponseStatus.COMPLETED:

            rpc.set_completed()
            self._rpc_manager.close_request(rpc.uuid)

            if task.next_step():

                rpc = self._rpc_manager.put_request(task.current_request(), task.payload)
                if rpc.status == RPCStatus.WAITING:
                    task_data.requests.append(rpc)
                    del self._requests[response.request_id]
                    self._requests[rpc.uuid] = task_data.task.uuid()
                else:
                    task.unroll()
                    # Here the place to log put request failed
                    self._closed_tasks = (task_uuid, task_data)
                    #
                    del self._tasks[task_uuid]
                    self._rpc_manager.close_request(rpc.uuid)

            else:
                task.close()
                #  Here is the place to log task completeness
                self._closed_tasks = (task_uuid, task_data)
                #
                del self._tasks[task_uuid]
                del self._requests[response.request_id]

        self._task_logger.update_task(task_data)

    def run_in_external_ioloop(self, io_loop):

        self._rpc_manager = RPCManager(RPCManager.CLIENT, ampq_url=self._ampq_url,
                                       heart_bit_timeout=5,
                                       reply_callback=self.update_task_status,
                                       error_callback=self.ErrorCallbackHandler(self._task_logger))

        self._rpc_manager.run_async(io_loop)




