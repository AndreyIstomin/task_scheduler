"""
Useful articles:
https://www.cloudcity.io/blog/2019/02/27/things-i-wish-they-told-me-about-multiprocessing-in-python/
https://pika.readthedocs.io/en/stable/modules/channel.html
https://pymotw.com/3/asyncio/executors.html
"""


import uuid
import time
import json
import jsonschema
from enum import Enum
from multiprocessing import Pipe
from multiprocessing.connection import Connection, wait
from PluginEngine import Log
from PluginEngine.common import require, empty_uuid
from backend.task_scheduler_service import Scenario
from backend.task_scheduler_service.common import ResponseObject, uuid_to_array, array_to_uuid
from backend.task_scheduler_service.schemas import CMD_MESSAGE_SCHEMA


class CMDTypeEnum:

    OK, CLOSE_TASK = __list = range(2)

    def __iter__(self):
        return self.__list.__iter__()


CMDType = CMDTypeEnum()


class RPCManagerCMD:

    def __init__(self, cmd: int, request_id: str, username: str):

        self.type = cmd
        self.request_id = uuid.UUID(request_id)
        self.user = username

    def to_json(self):

        return json.dumps({
            'cmd': self.type,
            'request_id': str(self.request_id),
            'username': self.user
        })

    @classmethod
    def from_json(cls, json_data: bytes):

        d = json.loads(json_data)
        jsonschema.validate(d, CMD_MESSAGE_SCHEMA)
        if not d['cmd'] in CMDType:
            raise jsonschema.ValidationError(f'incorrect CMD type: {d["cmd"]}')
        return cls(**d)


CMD_WAIT_TIMEOUT_SEC = 0.02


class CMDHandlerMock:

    def __init__(self, conn: Connection):
        pass

    def try_open_task(self, task_uuid: uuid.UUID) -> bool:
        return True

    def is_task_close_requested(self):
        return False

    def notify_task_closed(self):
        pass


class CMDHandler:

    def __init__(self, conn: Connection):
        self._conn = conn
        self._task_started = False
        self._close_requested = False
        self._task_uuid = empty_uuid
        pass

    def try_open_task(self, task_uuid: uuid.UUID) -> bool:
        """
        Waits until the server reply OK or CLOSE_TASK command
        :param task_uuid: task uuid
        :return: Whether the task has not been requested to close
        """
        self._task_uuid = task_uuid
        self._task_started = True
        self._conn.send(str(self._task_uuid))
        if self._wait_reply() != CMDType.OK:
            self._reset()
            return False

        return True

    def is_task_close_requested(self):
        """
        Whether the task close has been requested by server;
        can only be used after the try_open_task has been called;
        blocks the execution for CMD_WAIT_TIMEOUT_SEC seconds
        """
        require(self._task_started)

        if not self._close_requested:
            self._close_requested = self._wait_reply_timed_out() == CMDType.CLOSE_TASK

        return self._close_requested

    def notify_task_closed(self):
        """
        Waits until the server replied
        """
        self._reset()
        self._wait_reply()

# private
    def _wait_reply(self):

        rsp = (0, 0)
        while rsp[1] != str(self._task_uuid):
            rsp = self._conn.recv()

        return rsp[1]

    def _wait_reply_timed_out(self):

        rsp = (0, 0)
        while rsp[1] != str(self._task_uuid):

            if self._conn.poll(timeout=CMD_WAIT_TIMEOUT_SEC):
                rsp = self._conn.recv()
            else:
                return None

        return rsp

    def _reset(self):

        self._task_uuid = empty_uuid
        self._task_started = False
        self._close_requested = False


class CMDManager:

    class ProcessDescriptor:

        def __init__(self, conn: Connection):
            self.conn = None
            self.task_uuid = empty_uuid
            self.close_requested = False

        def reset_task(self):

            self.task_uuid = empty_uuid
            self.close_requested = False

    def __init__(self):

        self._processes = {}
        self._active_tasks = {}
        self._close_requests = set()

    def close_request(self, task_uuid: uuid.UUID):
        require(isinstance(task_uuid, uuid.UUID))

        # The task has not been sent to consumer yet
        if task_uuid not in self._active_tasks:
            self._close_requests.add(task_uuid)

        # Task is already consuming
        else:
            process = self._processes[self._active_tasks[task_uuid]]
            if not process.close_requested:
                process.close_requested = True
                process.conn.send([CMDType.CLOSE_TASK, str(task_uuid)])

    def create_cmd_handler(self, process_id: int):
        require(process_id not in self._processes)

        parent_conn, child_conn = Pipe()
        self._processes[process_id] = self.ProcessDescriptor(parent_conn)

        return CMDHandler(child_conn)

    def remove_cmd_handler(self, process_id: int):

        self._processes[process_id].conn.close()
        del self._processes[process_id]

    def run_in_loop(self, io_loop):

        pass

    def close(self):

        pass

# protected
    def _poll_processes(self):

        # for conn in wait([item.conn for item in self._processes], timeout=self.WAIT_TIMEOUT_SEC): TODO: check it
        for idx, item in self._processes.items():

            self._poll(idx, item)

    def _poll(self, idx: int, process: ProcessDescriptor):

        if process.conn.poll(CMD_WAIT_TIMEOUT_SEC):

            task_uuid = uuid.UUID(process.conn.recv())

            if task_uuid != empty_uuid:

                require(task_uuid not in self._active_tasks)

                if task_uuid not in self._close_requests:
                    process.conn.send([CMDType.OK, str(task_uuid)])
                    process.task_uuid = task_uuid
                    self._active_tasks[task_uuid] = idx

                else:
                    process.conn.send([CMDType.CLOSE_TASK, str(task_uuid)])
                    process.reset_task()
                    self._close_requests.discard(task_uuid)

            else:

                require(task_uuid in self._active_tasks)

                process.conn.send([CMDType.OK, str(empty_uuid)])
                process.reset_task()
                del self._active_tasks[task_uuid]
                self._close_requests.discard(task_uuid)


class RPCBase:

    class ConsumerAlreadyRegisteredException(Exception):
        pass

    EXCHANGE = 'rpc_manager_exchange'
    CMD_EXCHANGE = 'rpc_manager_cmd_exchange'
    CMD_QUEUE = 'rpc_manager_cmd_queue'
    CMD_ROUTING_KEY = 'rpc_manager_cmd'

    PREFETCH_COUNT = 1

    _known_consumers = {}

    @classmethod
    def is_consumer(cls, routing_key):
        require(isinstance(routing_key, str))

        def register(class_):
            if routing_key not in cls._known_consumers:
                Log.debug(f'{routing_key} has been registered as RPC consumer')
                class_._routing_key = routing_key
                cls._known_consumers[class_.get_routing_key()] = class_

            else:
                raise RPCBase.ConsumerAlreadyRegisteredException(f'{routing_key} is already registered')

            return class_

        return register

    @classmethod
    def check_scenario(cls, scenario: 'Scenario') -> (bool, str):

        error_msg = ','.join(request for request in scenario if request not in cls._known_consumers)

        ok = error_msg == ''
        return ok, 'Ok' if ok else f'Incorrect scenario {scenario.name()}, unknown requests: ' + error_msg


class ReplyCallbackInterface:

    def __call__(self, response: ResponseObject):
        raise NotImplementedError()


class RPCErrorCallbackInterface:

    def on_response_json_decode_error(self, ex: Exception):
        raise NotImplementedError

    def on_response_json_validation_error(self, ex: Exception):
        raise NotImplementedError

    def on_unknown_request_id(self):
        raise NotImplementedError


class RPCStatus:
    INACTIVE, WAITING, IN_PROGRESS, COMPLETED, FAILED = 0, 1, 2, 3, 4

    @staticmethod
    def verbose(status: int):
        return ['inactive', 'waiting', 'in progress', 'completed', 'failed'][status]


class RPCData:

    def __init__(self, request_id: uuid.UUID, routing_key: str, progress: float, status: RPCStatus, message: str):

        require(isinstance(request_id, uuid.UUID))
        self.uuid = request_id
        self.routing_key = routing_key
        self.progress = progress
        self.status = status
        self.message = message
        self._heartbit_time = None

    def update_heartbit_time(self):

        if self.status is RPCStatus.WAITING:
            self.status = RPCStatus.IN_PROGRESS

        self._heartbit_time = time.time()

    def set_completed(self):
        self.status = RPCStatus.COMPLETED

    def set_failed(self):
        self.status = RPCStatus.FAILED

