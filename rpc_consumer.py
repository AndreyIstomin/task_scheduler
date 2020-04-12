import uuid
import json
import pika
from functools import wraps
from PluginEngine import Log, ProgressInterface
from PluginEngine.common import require
from LandscapeEditor.backend import RPCConsumerInterface, GeneratorInterface, CloseRequestException
from backend.task_scheduler_service import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase, CMDHandler
from backend.generator_service import create_client_notifier, create_db_handler


class RPCConsumerInput:

    def __init__(self, amqp_url: str, heartbit_timeout: 'seconds', instance_id: int, cmd_handler: CMDHandler):

        self.amqp_url = amqp_url
        self.heartbit_timeout = heartbit_timeout
        self.instance_id = instance_id
        self.cmd_handler = cmd_handler


class RPCConsumerExitCmd(Exception):
    pass


class RPCProgressHandler(ProgressInterface):

    def __init__(self, *args, **kwargs):
        ProgressInterface.__init__(self, *args, **kwargs)

        self._steps_done = 0

    def update(self, tasks_completed):

            if not self._total_count:
                self._progress = 100
                return self._progress

            self._task_completed += tasks_completed

            if self._task_completed < self._total_count:
                self._progress = int(self._task_completed / float(self._total_count) * 100.0)
            else:
                self._progress = 100

            steps_done = int(self._progress / self._step)

            if steps_done != self._steps_done:
                self._steps_done = steps_done
                self._logger.publish_progress(self._progress * 0.01, self._name)

            return self._progress


class RPCConsumer(RPCBase, RPCConsumerInterface):

    _routing_key = None

    """
    If enabled, CloseRequestException is raised on forced close request;
    A default value must be False;
    """
    _raise_on_close_request = False

    def _check_close_requested(self):
        pass

    def _raise_if_close_requested(self):
        if self.is_close_requested():
            raise CloseRequestException()

    def __new__(cls, *args, **kwargs):
        if cls._raise_on_close_request:
            cls._check_close_requested = cls._raise_if_close_requested

        return super(RPCConsumer, cls).__new__(cls, *args, **kwargs)

    def _run_task(self):
        raise NotImplementedError()

    def __init__(self):
        self._connection = None
        self._channel = None
        self._consumer_input = None
        self._cmd_handler = None
        self._raise_on_close_req = False

        self._ch = None
        self._method = None
        self._properties = None
        self._payload = None

        self._task_is_opened = False
        self._task_is_closed = False
        self._progress = 0.0
        self._failed = False
        self._message = ""
        self._complete_message = None
        self._err_message = None

#  RPCConsumerInterface ################################################################################################

    def raise_on_close_request(self, enabled: bool):
        self._raise_on_close_req = enabled

    def is_close_requested(self):
        return self._cmd_handler.is_task_close_requested()

    def instance_id(self) -> int:
        return self._consumer_input.instance_id

    def payload(self) -> dict:
        return self._payload

    def publish_progress(self, progress: '[0.0, 1.0]', message=None):

        if message is not None:
            self._message = message
        self._progress = progress
        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.IN_PROGRESS, progress=self._progress, message=self._message)

        self._publish_response(response)

    def publish_message(self, message: str):

        self._message = message
        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.IN_PROGRESS, progress=self._progress, message=self._message)

        self._publish_response(response)

    def create_progress(self, step_count: int, msg: str, step: int) -> ProgressInterface:
        """

        :param step_count:
        :param msg:
        :param step:
        :return:
        """
        return RPCProgressHandler(self, step_count, msg, step)

    def notify_task_completed(self, message: str):

        self._complete_message = message

    def notify_task_failed(self, err_message: str):

        self._failed = True
        self._err_message = err_message

#  Protected ###########################################################################################################

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                         routing_key=self._properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                         body=response.to_json())

        self._check_close_requested()

    def _publish_completed(self):

        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.COMPLETED, progress=1.0, message=self._complete_message or
                                  f'{self._routing_key} has completed the task')

        if self._complete_message is None:
            Log.warn(f'{self._routing_key}: undefined complete message')

        self._publish_response(response)

    def _publish_error(self):
        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.FAILED, progress=0.0, message=self._err_message or
                                  f'{self._routing_key} failed')

        self._publish_response(response)

    def _notify_task_opened(self):

        if not self._task_is_opened:
            self.publish_message('task is opened')
            self._task_is_opened = True

        self._check_close_requested()

    def _notify_task_closed(self):

        if not self._task_is_closed:
            self._ch.basic_ack(delivery_tag=self._method.delivery_tag)
            self._task_is_closed = True

        self._check_close_requested()

    def _reset_state(self):
        self._task_is_opened = False
        self._task_is_closed = False
        self._progress = 0.0
        self._failed = False
        self._message = ""
        self._complete_message = None
        self._err_message = None

    def _callback(self, ch, method, properties, body):

        Log.info(f"The {self.instance_id()}th test RPC consumer got task")

        self._reset_state()

        #  First of all update vars
        self._ch = ch
        self._method = method
        self._properties = properties

        # Check input
        try:
            self._payload = json.loads(body)
        except json.JSONDecodeError as err:
            Log.error(f"Consumer {self.get_routing_key()} incorrect input data: {err}")
            self._err_message = f"Incorrect input data: {err}"
            self._publish_error()
            self._notify_task_closed()
            return

        # Run task
        self._notify_task_opened()
        try:
            self._run_task()

            if not self._failed:
                self._publish_completed()
            else:
                self._publish_error()
        except Exception as err:

            Log.error(f"Consumer {self.get_routing_key()} exception: {str(err)}")
            self._err_message = f"Exception {err}"
            self._publish_error()
        finally:
            self._notify_task_closed()

    def __callback(self, ch, method, properties, body):

        try:

            #  First of all update vars
            self._ch = ch
            self._method = method
            self._properties = properties
            self._reset_state()

            if not self._cmd_handler.try_open_task(self._properties.correlation_id):
                user = 'user1'  # TODO
                self.notify_task_failed(f'Task has been cancelled by user {user}')
                return

            Log.info(f"The {self.instance_id()}th test RPC consumer got task")

            # Check input
            try:
                self._payload = json.loads(body)
            except json.JSONDecodeError as err:
                Log.error(f"Consumer {self.get_routing_key()} incorrect input data: {err}")
                self.notify_task_failed(f"Incorrect input data: {err}")
                return

            # Run task
            self._notify_task_opened()
            self._run_task()

        except Exception as err:
            Log.error(f"Consumer {self.get_routing_key()} exception: {str(err)}")
            self._err_message = f"Exception {err}"
            self._publish_error()
        finally:
            if not self._failed:
                self._publish_completed()
            else:
                self._publish_error()
            self._notify_task_closed()
            self._cmd_handler.notify_task_closed()

    @classmethod
    def get_routing_key(cls):
        return cls._routing_key

    @classmethod
    def get_queue_name(cls):
        return f"{cls.get_routing_key()}_queue"

    def run(self, input_: RPCConsumerInput):
        """
        TODO: think of the RabbitMQ work to be delegated to RPCManager
        :param input_:
        :return:
        """
        self._consumer_input = input_
        self._cmd_handler = input_.cmd_handler
        self._connection = pika.BlockingConnection(
            pika.URLParameters(input_.amqp_url))
        self._channel = self._connection.channel()
        """durable=False: RabbitMQ WILL lose the queue and messages in it if crashes or quits """
        self._channel.queue_declare(queue=self.get_queue_name(), durable=False)
        self._channel.queue_bind(exchange=self.EXCHANGE, queue=self.get_queue_name(),
                                 routing_key=self.get_routing_key())
        self._channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
        self._channel.basic_consume(queue=self.get_queue_name(), on_message_callback=self.__callback, auto_ack=False)
        self._channel.start_consuming()


class GeneratorAdapter(RPCConsumer):

    def __init_subclass__(cls, generator_class: type, raise_on_close_request: bool):

        require(issubclass(generator_class, GeneratorInterface))

        cls.__generator = generator_class
        cls._raise_on_close_request = raise_on_close_request

        super(GeneratorAdapter, cls).__init_subclass__()

    def _run_task(self):

        generator = self.__generator(
            create_client_notifier(self._payload['username'],  self.__generator.process_name()),
            create_db_handler(), self._payload, self)
        generator._run()




