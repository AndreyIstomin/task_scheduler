import uuid
from PluginEngine import Log
from PluginEngine.common import require
from backend.task_scheduler_service.common import ResponseObject


class RPCBase:
    STOP_REQUEST_ROUTING_KEY = 'stop_request'
    EXCHANGE = 'rpc_manager'
    CONSUMER_QUEUE = 'rpc_queue'
    PREFETCH_COUNT = 1

    _known_consumers = {}

    @classmethod
    def is_consumer(cls, routing_key):
        require(isinstance(routing_key, str))

        def register(class_):
            if routing_key not in cls._known_consumers:
                Log.info(f'{routing_key} has been registered as {cls.__name__}')
                class_._routing_key = routing_key
                cls._known_consumers[class_.get_routing_key()] = class_

            else:
                raise Log.error(f'{routing_key} is already registered')

            return class_

        return register


class ReplyCallbackInterface:

    def __call__(self, response: ResponseObject):
        raise NotImplementedError()


class ExitCallbackInterface:

    def __call__(self, response: ResponseObject):
        raise NotImplementedError()


class RPCStatus:
    INACTIVE, IN_PROGRESS, COMPLETED, FAILED = [0, 1, 2, 3]

    @staticmethod
    def verbose(status: int):
        return ['inactive', 'in progress', 'completed', 'failed'][status]


class RPCData:

    def __init__(self, request_id: uuid.UUID, routing_key: str, progress: float, status: RPCStatus, message: str):

        require(isinstance(request_id, uuid.UUID))
        self.uuid = request_id
        self.routing_key = routing_key
        self.progress = progress
        self.status = status
        self.message = message
