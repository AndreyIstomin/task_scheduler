import uuid
import time
import json
from PluginEngine.common import require
from backend.task_scheduler_service import SchedulerAsyncPublisher
from backend.task_scheduler_service.common import ResponseObject, ResponseStatus


class RPCConsumer:

    pass


class ReplyCallbackInterface:

    def __call__(self, response: ResponseObject):
        raise NotImplementedError()

class ExitCallbackInterface:

    def __call__(self, response: ResponseObject):
        raise NotImplementedError()


class RPCManager:

    SERVER = 0
    CLIENT = 1

    STOP_REQUEST_ROUTING_KEY = 'stop_request'

    class Request:

        def __init__(self, owner_id: uuid.UUID):
            self.owner = owner_id
            self.heart_bit_time = self.start_time = time.time()
            self.running = True

    def __init__(self, regime, ampq_url: str, reply_callback: ReplyCallbackInterface,
                 exit_callback: ExitCallbackInterface,
                 heart_bit_timeout: 'seconds'):

        require(regime in [RPCManager.SERVER, RPCManager.CLIENT])

        self._regime = regime
        self._reply_callback = reply_callback
        self._exit_callback = exit_callback
        self._heart_bit_timeout = heart_bit_timeout
        self._running = False
        self._consumers = {}
        self._requests = {}
        self._regime = None
        self._io_loop = None
        self._ampq_url = ampq_url

        self._publisher = None

    #  Server interface
    def add_consumer(self, routing_key: str, consumer: RPCConsumer):

        require(self._regime == RPCManager.SERVER)
        require(not self._running)
        require(routing_key not in self._consumers)

        self._consumers[routing_key] = consumer

    # Client interface
    def put_request(self, routing_key: str, owner_id: uuid.UUID, payload: dict)-> (uuid.UUID, str):
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        d = dict(payload)
        d['owner_id'] = str(owner_id)

        if routing_key not in self._consumers:
            return None, 'Unknown routing key'

        json_payload = json.dumps(d)
        request_id = uuid.uuid4()
        self._publisher.publish_message(routing_key=routing_key, corr_id=request_id, payload=json_payload)
        self._requests[request_id] = RPCManager.Request(owner_id)

        return request_id, 'Ok'

    def close_request(self, request_id: uuid.UUID)-> (bool, str):

        if request_id in self._requests:
            """
            TODO: here we need to process correctly  confirmation, don't we?
            """
            self._publisher.publish_message(routing_key=RPCManager.STOP_REQUEST_ROUTING_KEY, corr_id=request_id,
                                            payload=None)
            del self._requests[request_id]
            return True, 'Ok'

        else:
            return False, 'Request not found'

    # Both
    def run(self, io_loop) -> (bool, str):
        self._io_loop = io_loop
        if self._regime == RPCManager.SERVER:
            return self._run_server()
        else:
            return self._run_client()

    # protected methods
    def _run_server(self) -> (bool, str):

        return False, 'Error'

    def _run_client(self) -> (bool, str):

        self._publisher = SchedulerAsyncPublisher(self._ampq_url, self._reply_callback)

        self._publisher.run_in_external_ioloop(self._io_loop)

        return True, 'Ok'

    def _feedback_callback(self, payload: bytes):
        """For test purposes"""
        print('Feedback callbakc: ' + str(bytes))

