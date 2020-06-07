import asyncio
import time
import uuid
import json
import jsonschema
import pika
from typing import *
from multiprocessing import Process
from PluginEngine import Log
from PluginEngine.common import empty_uuid
from PluginEngine.asserts import require
from LandscapeEditor.backend import TaskInputInterface
from backend.task_scheduler_service import SchedulerAsyncPublisher, SchedulerAsyncConsumer, RPCConsumerInput
from backend.task_scheduler_service.common import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase, RPCRegistry, RPCData, RPCStatus, \
    RPCErrorCallbackInterface, RPCManagerCMD, CMDManager, CMDType


class RPCConsumerData:

    def __init__(self, routing_key: str, instance_count: int):
        self.routing_key = routing_key
        self.instance_count = instance_count


class ProcessContainer:

    def __init__(self, process: Process, routing_key: str, instance_id: int):
        self.process = process
        self.routing_key = routing_key
        self.instance_id = instance_id


class RPCManager(RPCRegistry):

    SERVER = 0
    CLIENT = 1
    STOP_WAIT_SEC = 10
    PROCESS_RESTART_DELAY = 1

    class Request:

        def __init__(self):
            pass

    def __init__(self, regime, amqp_url: str, reply_callback=None,
                 error_callback=None):

        require(regime in [RPCManager.SERVER, RPCManager.CLIENT])

        if regime is RPCManager.CLIENT:
            require(isinstance(error_callback, RPCErrorCallbackInterface))

        # Common variables
        self._regime = regime
        self._running = False
        self._io_loop = None
        self._amqp_url = amqp_url

        # Client variables
        self._requests: Dict[uuid.UUID, RPCManager.Request] = {}
        self._ext_reply_callback = reply_callback
        self._ext_error_callback = error_callback
        self._publisher = None

        # Server variables
        self._consumers: List[RPCConsumerData] = []
        self._processes: List[ProcessContainer] = []
        self._cmd_consumer = None
        self._cmd_manager = None
        self._closing = False

    #  Server interface
    def add_consumer(self, routing_key: str, instance_count: int):

        require(self._regime == RPCManager.SERVER)
        require(not self._running)
        require(routing_key in self._known_consumers, f'unknown consumer: {routing_key}')
        self._consumers.append(RPCConsumerData(routing_key, instance_count))

    # Client interface
    def request(self, routing_key: str, task_input: TaskInputInterface) -> RPCData:
        require(self._regime == RPCManager.CLIENT)
        require(self._publisher)
        require(self._publisher.running())

        if routing_key not in self._known_consumers:
            return RPCData(empty_uuid, routing_key, 0.0, RPCStatus.FAILED, 'Unknown routing key')

        json_payload = json.dumps(task_input.to_dict())
        request_id = uuid.uuid4()
        self._publisher.publish_message(exchange=self.EXCHANGE, routing_key=routing_key, corr_id=request_id,
                                        payload=json_payload)
        self._requests[request_id] = RPCManager.Request()

        return RPCData(request_id, routing_key, 0.0, RPCStatus.WAITING, 'The request has been sent')

    def close_request(self, request_id: uuid.UUID, username: str, terminate: bool = False) -> (bool, str):
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
                payload=json.dumps(RPCManagerCMD(CMDType.CLOSE_TASK if not terminate else CMDType.TERMINATE_TASK,
                                                 str(request_id), username).to_json()))
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
            return self._run_server_async(io_loop)
        else:
            return self._run_client()

    def run(self) -> (bool, str):
        require(self._regime == RPCManager.SERVER)
        return self._run_server()

    def terminate_process(self, process_id: int):
        require(self._regime == RPCManager.SERVER)
        self._processes[process_id].process.terminate()
        Log.warn(f'Process {process_id} has been terminated')

    def publish_reply(self, response: ResponseObject):
        require(self._regime == RPCManager.SERVER)
        require(isinstance(response, ResponseObject))
        self._cmd_consumer.channel().basic_publish(
            exchange=RPCBase.EXCHANGE,
            routing_key=SchedulerAsyncPublisher.REPLY_ROUTING_KEY,
            body=response.to_json())

    # protected methods
    @staticmethod
    def _run_consumer(class_, input_: RPCConsumerInput):

        consumer = class_()
        consumer.run(input_)

    def _stop_server(self):

        # self.shutdown_event.set()
        self._closing = True
        end_time = time.time() + self.STOP_WAIT_SEC
        num_terminated = 0
        num_failed = 0

        # -- Wait up to STOP_WAIT_SECS for all processes to complete
        for item in self._processes:
            join_secs = max(0.0, min(end_time - time.time(), self.STOP_WAIT_SEC))
            item.process.join(join_secs)

        # -- Clear the procs list and _terminate_ any procs that
        # have not yet exited
        while self._processes:
            item = self._processes.pop()
            if item.process.is_alive():
                item.process.terminate()
                num_terminated += 1
            else:
                exitcode = item.process.exitcode
                if exitcode:
                    num_failed += 1

        Log.info(f'RPC server stopped: {num_failed} failed and {num_terminated} terminated processes')

        return num_failed, num_terminated

    def _create_process(self, routing_key: str, proc_id: int, instance_id: int):

        return ProcessContainer(
            process=Process(target=RPCManager._run_consumer,
                            args=(self._known_consumers[routing_key],
                                  RPCConsumerInput(
                                      self._amqp_url,
                                      instance_id,
                                      self._cmd_manager.create_cmd_handler(process_id=proc_id)))),
            routing_key=routing_key,
            instance_id=instance_id
        )

    def _restart_process(self, proc_id: int):
        item = self._processes[proc_id]
        require(not item.process.is_alive())
        self._cmd_manager.notify_process_is_broken(proc_id)
        self._processes[proc_id] = self._create_process(item.routing_key, proc_id, item.instance_id)
        self._processes[proc_id].process.start()
        Log.warn(f'Process: {item.routing_key}, instance {item.instance_id} has been restarted')

    def _run_server(self) -> (bool, str):

        require(self._regime == RPCManager.SERVER)
        self._cmd_manager = CMDManager(self)
        #  Here we implement the most easiest solution - blocking consuming
        self._processes = []
        for consumer_data in self._consumers:
            for instance_id in range(consumer_data.instance_count):
                self._processes.append(
                    self._create_process(consumer_data.routing_key, len(self._processes), instance_id))
                self._processes[-1].process.start()

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
        asyncio.get_event_loop().create_task(self._keep_processes_running())

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

    async def _keep_processes_running(self):

        while not self._closing:
            for proc_id, item in enumerate(self._processes):
                if not item.process.is_alive():
                    self._restart_process(proc_id)

            await asyncio.sleep(RPCManager.PROCESS_RESTART_DELAY)

    def _on_cmd_message(self, body, correlation_id):
        try:
            cmd = RPCManagerCMD.from_json(json.loads(body))
        except json.JSONDecodeError as err:
            Log.error(f'Incorrect JSON: {err}')
            return
        except jsonschema.ValidationError as err:
            Log.error(f'Incorrect JSON format: {err}')
            return

        if cmd.type == CMDType.CLOSE_TASK:
            Log.trace('RPC server got close task command')
            self._cmd_manager.close_request(cmd.request_id)

        elif cmd.type == CMDType.NOTIFY_TASK_CLOSED:
            Log.trace('RPC server got task close notification')
            self._cmd_manager.cancel_close_request(cmd.request_id)

        elif cmd.type == CMDType.TERMINATE_TASK:
            Log.trace('RPC server got task termination command')
            self._cmd_manager.terminate_request(cmd.request_id)
        else:
            Log.warn(f'RPC server got unknown command: {cmd.type}')

    def _run_server_async(self, io_loop) -> (bool, str):

        return False, 'Async consuming is not implemented'

    def _run_client(self) -> (bool, str):

        self._publisher = SchedulerAsyncPublisher(self._amqp_url, self._reply_callback, self.EXCHANGE)

        self._publisher.run_in_external_ioloop(self._io_loop)

        return True, 'Ok'

    def _try_close_request(self, correlation_id):
        try:
            self.close_request(uuid.UUID(correlation_id), 'task_scheduler', terminate=True)
        except Exception as err:
            Log.warn(f'Failed to close invalid request: {err}')

    def _reply_callback(self, payload: bytes, correlation_id: str):
        #  For test purposes
        # Log.info('Feedback callback: ' + str(payload))

        try:
            response = ResponseObject.from_json(payload)
        except json.JSONDecodeError as err:
            #  TODO: process the exception
            Log.error(f"Invalid JSON of the response: {err}")
            self._ext_error_callback.on_response_json_decode_error(err)
            self._try_close_request(correlation_id)
            return
        except jsonschema.ValidationError as err:
            Log.error(f"Incorrect JSON format of the response: {err}")
            self._ext_error_callback.on_response_validation_error(err)
            self._try_close_request(correlation_id)
            return

        if response.request_id not in self._requests:
            Log.error(f"Unknown RPC request {response.request_id}")
            self._ext_error_callback.on_unknown_request_id()

        self._ext_reply_callback(response)



