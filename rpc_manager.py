import asyncio
import time
import uuid
import json
import jsonschema
import pika
from multiprocessing import Process, Value
from PluginEngine import Log
from PluginEngine.common import require, empty_uuid
from backend.task_scheduler_service import SchedulerAsyncPublisher, SchedulerAsyncConsumer, RPCConsumerInput
from backend.task_scheduler_service.common import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase, RPCData, RPCStatus, RPCErrorCallbackInterface, \
    RPCManagerCMD, CMDManager, CMDHandler, CMDHandlerMock, CMDType


class RPCConsumerData:

    def __init__(self, routing_key: str, instance_count: int):
        self.routing_key = routing_key
        self.instance_count = instance_count


class RPCManager(RPCBase):

    SERVER = 0
    CLIENT = 1
    STOP_WAIT_SEC = 10

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
        self._consumers = []
        self._processes = []
        self._cmd_consumer = None
        self._cmd_manager = None

    #  Server interface
    def add_consumer(self, routing_key: str, instance_count: int):

        require(self._regime == RPCManager.SERVER)
        require(not self._running)
        require(routing_key in self._known_consumers)
        self._consumers.append(RPCConsumerData(routing_key, instance_count))

    # Client interface
    def request(self, routing_key: str, payload: dict) -> RPCData:
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

    def close_request(self, request_id: uuid.UUID, username: str) -> (bool, str):
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        if request_id in self._requests:
            """
            TODO: here we need to process correctly  confirmation, don't we?
            """

            self._publisher.publish_message(
                exchange=self.CMD_EXCHANGE, routing_key=RPCBase.CMD_ROUTING_KEY,
                corr_id=request_id,
                payload=json.dumps(RPCManagerCMD(CMDType.CLOSE_TASK, str(request_id), username).to_json()))
            # del self._requests[request_id]

            # HERE WE ARE
            return True, 'Ok'

        else:
            return False, 'Request not found'

    def notify_task_closed(self, request_id: uuid.UUID, username: str) -> (bool, str):
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        if request_id in self._requests:

            self._publisher.publish_message(
                exchange=self.CMD_EXCHANGE, routing_key=RPCBase.CMD_ROUTING_KEY,
                corr_id=request_id,
                payload=json.dumps(RPCManagerCMD(CMDType.NOTIFY_TASK_CLOSED, str(request_id), username).to_json()))
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
    def _run_consumer(class_, input_: RPCConsumerInput):

        consumer = class_()
        consumer.run(input_)

    def _stop_server(self):

        # self.shutdown_event.set()

        end_time = time.time() + self.STOP_WAIT_SEC
        num_terminated = 0
        num_failed = 0

        # -- Wait up to STOP_WAIT_SECS for all processes to complete
        for proc in self._processes:
            join_secs = max(0.0, min(end_time - time.time(), self.STOP_WAIT_SEC))
            proc.join(join_secs)

        # -- Clear the procs list and _terminate_ any procs that
        # have not yet exited
        while self._processes:
            proc = self._processes.pop()
            if proc.is_alive():
                proc.terminate()
                num_terminated += 1
            else:
                exitcode = proc.exitcode
                if exitcode:
                    num_failed += 1

        Log.info(f'RPC server stopped: {num_failed} failed and {num_terminated} terminated processes')

        return num_failed, num_terminated

    def _run_server(self) -> (bool, str):

        require(self._regime == RPCManager.SERVER)
        self._cmd_manager = CMDManager()
        #  Here we implement the most easiest solution - blocking consuming
        self._processes = []
        for consumer_data in self._consumers:
            for instance_id in range(consumer_data.instance_count):

                process = Process(
                    target=RPCManager._run_consumer,
                    args=(self._known_consumers[consumer_data.routing_key],
                          RPCConsumerInput(
                              self._amqp_url, self._heart_bit_timeout, instance_id,
                              self._cmd_manager.create_cmd_handler(process_id=len(self._processes)))))

                self._processes.append(process)
                process.start()

        #  Starting command queue(async)
        self._cmd_consumer = SchedulerAsyncConsumer(self._amqp_url, self._on_cmd_message)
        self._cmd_consumer.EXCHANGE = self.CMD_EXCHANGE
        self._cmd_consumer.EXCHANGE_TYPE = 'fanout'
        self._cmd_consumer.EXCHANGE_DURABLE = False
        self._cmd_consumer.EXCHANGE_AUTO_DELETE = True
        self._cmd_consumer.QUEUE = ''
        self._cmd_consumer.QUEUE_AUTO_DELETE = True
        self._cmd_consumer.ROUTING_KEY = self.CMD_ROUTING_KEY
        loop = asyncio.get_event_loop()
        self._cmd_consumer.run_in_loop(loop)
        self._cmd_manager.run_in_loop(loop)

        try:
            loop.run_forever()
        except Exception as ex:
            Log.error(f'RPC server exception: {ex}')
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            self._stop_server()
        ####
        #  Blocking connection
        # try:
        #     self._connection = pika.BlockingConnection(
        #         pika.URLParameters(self._amqp_url))
        #     self._channel = self._connection.channel()
        #
        #     self._channel.exchange_declare(
        #         self.CMD_EXCHANGE, exchange_type='fanout', passive=False, durable=False,
        #         auto_delete=True, internal=False, arguments=None)
        #
        #     """durable=False: RabbitMQ WILL lose the queue and messages in it if crashes or quits """
        #     self._channel.queue_declare(queue=self.CMD_QUEUE, durable=False)
        #     self._channel.queue_bind(exchange=self.CMD_EXCHANGE, queue=self.CMD_QUEUE,
        #                              routing_key=self.CMD_ROUTING_KEY)
        #     self._channel.basic_qos(prefetch_count=1)
        #     self._channel.basic_consume(queue=self.CMD_QUEUE, on_message_callback=self._on_cmd_message, auto_ack=True)
        #     self._channel.start_consuming()
        # finally:
        #     self._connection.close()
        #     self._stop_server()
        ####

        return True, 'Ok'

    def _on_cmd_message(self, body):
        try:
            cmd = RPCManagerCMD.from_json(json.loads(body))
        except json.JSONDecodeError as err:
            Log.error(f'Incorrect JSON: {err}')
            return
        except jsonschema.ValidationError as err:
            Log.error(f'Incorrect JSON format: {err}')
            return

        if cmd.type == CMDType.CLOSE_TASK:
            self._cmd_manager.close_request(cmd.request_id)

        elif cmd.type == CMDType.NOTIFY_TASK_CLOSED:
            Log.trace('RPC Server got task close notification')
            self._cmd_manager.cancel_close_request(cmd.request_id)

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



