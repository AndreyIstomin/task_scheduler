import uuid
import time
import json
import jsonschema
import asyncio
from multiprocessing import Process, Value
from PluginEngine import Log
from PluginEngine.common import require, empty_uuid
from backend.task_scheduler_service import SchedulerAsyncPublisher, SchedulerAsyncConsumer, RPCConsumerInput
from backend.task_scheduler_service.common import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase, RPCData, RPCStatus, RPCErrorCallbackInterface


class RPCConsumerData:

    def __init__(self, instance_count: int):
        self.instance_count = instance_count
        self.processes = []


class RPCManager(RPCBase):

    SERVER = 0
    CLIENT = 1

    class Request:

        def __init__(self):
            pass

    def __init__(self, regime, amqp_url: str, heart_bit_timeout: 'seconds', reply_callback=None,
                 error_callback=None):

        require(regime in [RPCManager.SERVER, RPCManager.CLIENT])

        if regime is RPCManager.CLIENT:
            require(isinstance(error_callback, RPCErrorCallbackInterface))

        # Common variables
        self._regime = regime
        self._running = False
        self._heart_bit_timeout = heart_bit_timeout
        self._io_loop = None
        self._amqp_url = amqp_url

        # Client variables
        self._requests = {}
        self._ext_reply_callback = reply_callback
        self._ext_error_callback = error_callback
        self._publisher = None

        # Server variables
        self._consumers = {}
        self._stop_consumer = None

    #  Server interface
    def add_consumer(self, routing_key: str, instance_count: int):

        require(self._regime == RPCManager.SERVER)
        require(not self._running)
        require(routing_key in self._known_consumers)
        require(routing_key not in self._consumers)

        self._consumers[routing_key] = RPCConsumerData(instance_count)

    # Client interface
    def put_request(self, routing_key: str, payload: dict) -> RPCData:
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        d = dict(payload)

        if routing_key not in self._known_consumers:
            return RPCData(empty_uuid, routing_key, 0.0, RPCStatus.FAILED, 'Unknown routing key')

        json_payload = json.dumps(d)
        request_id = uuid.uuid4()
        self._publisher.publish_message(exchange=self.EXCHANGE, routing_key=routing_key, corr_id=request_id, payload=json_payload)
        self._requests[request_id] = RPCManager.Request()

        return RPCData(request_id, routing_key, 0.0, RPCStatus.WAITING, 'The request has been sent')

    def close_request(self, request_id: uuid.UUID) -> (bool, str):
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        if request_id in self._requests:
            """
            TODO: here we need to process correctly  confirmation, don't we?
            """
            self._publisher.publish_message(exchange=self.EXCHANGE, routing_key=RPCBase.CMD_ROUTING_KEY,
                                            corr_id=request_id, payload=None)
            # del self._requests[request_id]

            # HERE WE ARE
            return True, 'Ok'

        else:
            return False, 'Request not found'

    # Both
    def run_async(self, io_loop) -> (bool, str):
        self._io_loop = io_loop
        if self._regime == RPCManager.SERVER:
            return self._run_server_async()
        else:
            return self._run_client()

    def run(self) -> (bool, str):
        require(self._regime == RPCManager.SERVER)
        return self._run_server()

    # protected methods
    @staticmethod
    def _run_consumer(class_, input_:RPCConsumerInput):

        consumer = class_()
        consumer.run(input_)

    def _run_server(self) -> (bool, str):

        require(self._regime == RPCManager.SERVER)
        #  Here we implement the most easiest solution - blocking consuming
        processes = []
        for _type, consumer_data in self._consumers.items():
            for i in range(consumer_data.instance_count):

                process = Process(target=RPCManager._run_consumer,
                                  args=(self._known_consumers[_type],
                                        RPCConsumerInput(self._amqp_url, self._heart_bit_timeout, i))
                                  )
                consumer_data.processes.append(process)
                processes.append(process)
                process.start()

        #  Starting command queue
        self._stop_consumer = SchedulerAsyncConsumer(self._amqp_url, self._on_cmd_message)
        self._stop_consumer.EXCHANGE = self.CMD_EXCHANGE
        self._stop_consumer.EXCHANGE_TYPE = 'fanout'
        self._stop_consumer.QUEUE = ''
        self._stop_consumer.ROUTING_KEY = self.CMD_ROUTING_KEY
        loop = asyncio.get_event_loop()
        self._stop_consumer.run_in_external_ioloop(loop)

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            for process in processes:
                process.join()
        ####

        return True, 'Ok'

    def _on_cmd_message(self, payload: bytes):

        Log.info(f'cmd message: {payload}')

    def _run_server_async(self, io_loop) -> (bool, str):

        return False, 'Async consuming is not implemented'

    def _run_client(self) -> (bool, str):

        self._publisher = SchedulerAsyncPublisher(self._amqp_url, self._reply_callback, self.EXCHANGE)

        self._publisher.run_in_external_ioloop(self._io_loop)

        return True, 'Ok'

    def _reply_callback(self, payload: bytes):
        #  For test purposes
        # Log.info('Feedback callback: ' + str(payload))

        try:
            response = ResponseObject.from_json(payload)
        except json.JSONDecodeError as err:
            #  TODO: process the exception
            Log.error(f"Invalid JSON of the response: {err}")
            self._ext_error_callback.on_response_json_decode_error(err)
            return
        except jsonschema.ValidationError as err:
            Log.error(f"Incorrect JSON format of the response: {err}")
            self._ext_error_callback.on_response_validation_error(err)
            return

        if response.request_id not in self._requests:
            Log.error(f"unknown RPC request {response.request_id}")
            self._ext_error_callback.on_unknown_request_id()

        if response.status is ResponseStatus.IN_PROGRESS:
            pass

        elif response.status is ResponseStatus.COMPLETED:
            pass

        elif response.status is ResponseStatus.FAILED:
            pass

        self._ext_reply_callback(response)



