import uuid
import time
import json
import jsonschema
from enum import Enum
from PluginEngine import Log
from PluginEngine.common import require
from backend.task_scheduler_service import Scenario
from backend.task_scheduler_service.common import ResponseObject
from backend.task_scheduler_service.schemas import CMD_MESSAGE_SCHEMA


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


class CMDType(Enum):

    CLOSE_TASK = 0


class RPCManagerCMD:

    def __init__(self, cmd_type: int, request_id: str):

        self.type = cmd_type
        self.request_id = uuid.UUID(request_id)

    def to_json(self):

        return json.dumps({
            'cmd': str(self.type),
            'request_id': self.request_id})

    @classmethod
    def from_json(cls, json_data: bytes):

        d = json.loads(json_data)
        jsonschema.validate(d, CMD_MESSAGE_SCHEMA)
        if not d['cmd'] in CMDType:
            raise jsonschema.ValidationError(f'incorrect CMD type: {d["cmd"]}')
        return cls(**d)
