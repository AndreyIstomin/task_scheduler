import json
import pika
import sys,traceback
from abc import ABC, abstractmethod
from typing import *
from PluginEngine import Log, ProgressInterface
from PluginEngine.asserts import require
from LandscapeEditor.backend import RPCConsumerInterface, Generator, CloseRequestException, TaskInputInterface
from backend.task_scheduler_service import ResponseObject, ResponseStatus
from backend.task_scheduler_service.task_manager_common import TaskInput
from backend.task_scheduler_service.rpc_common import RPCBase, CMDHandler
from backend.generator_service import create_client_notifier, create_db_handler


class RPCConsumerInput:

    def __init__(self, amqp_url: str, instance_id: int, cmd_handler: CMDHandler):

        self.amqp_url = amqp_url
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

    @abstractmethod
    def _run_task(self):
        pass

    def __init__(self):
        self._connection = None
        self._channel = None
        self._consumer_input = None
        self._cmd_handler = None
        self._raise_on_close_req = False

        self._ch = None
        self._method = None
        self._properties = None
        self._input = None

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

    def input(self) -> TaskInputInterface:
        return self._input

    def publish_progress(self, progress: '[0.0, 1.0]', message=None):

        if message is not None:
            self._message = message
        self._progress = progress
        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.IN_PROGRESS, progress=self._progress, message=self._message)

        self._publish_response(response)
        self._check_close_requested()

    def publish_message(self, message: str):

        self._message = message
        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.IN_PROGRESS, progress=self._progress, message=self._message)

        self._publish_response(response)
        self._check_close_requested()

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

    @classmethod
    @abstractmethod
    def heartbit_timeout(cls):
        pass

    @classmethod
    @abstractmethod
    def validate_input(cls, data: Any) -> Tuple[bool, str]:
        pass


#  Protected ###########################################################################################################

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                         routing_key=self._properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                         body=response.to_json())

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

    def _notify_task_closed(self):

        if not self._task_is_closed:
            self._ch.basic_ack(delivery_tag=self._method.delivery_tag)
            self._task_is_closed = True

    def _reset_state(self):
        self._task_is_opened = False
        self._task_is_closed = False
        self._progress = 0.0
        self._failed = False
        self._message = ""
        self._complete_message = None
        self._err_message = None

    def _callback(self, ch, method, properties, body):

        try:

            #  First of all update vars
            self._ch = ch
            self._method = method
            self._properties = properties
            self._reset_state()

            if not self._cmd_handler.try_open_task(self._properties.correlation_id):
                user = 'undefined'  # TODO
                self.notify_task_failed(f'Task has been cancelled by user {user}')
                return

            Log.info(f"The {self.instance_id()}th test RPC consumer got task")

            # Check input
            try:
                data = json.loads(body)
                ok, msg = self.validate_input(data)
                if not ok:
                    Log.error(f"Incorrect JSON format: {msg}")
                    self.notify_task_failed(f"Incorrect JSON format: {msg}")
                    return

                self._input = TaskInput.from_dict(data)
            except json.JSONDecodeError as err:
                Log.error(f"Incorrect JSON: {err}")
                self.notify_task_failed(f"Incorrect JSON: {err}")
                return

            # Run task
            self._notify_task_opened()
            self._run_task()

        except CloseRequestException:

            user = 'undefined'  # TODO!!!
            self.notify_task_failed(f'Interrupted by {user}')

        except Exception as err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = "Consumer {key}\n{exc_info}".format(
                key=self.get_routing_key(),
                exc_info='\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            )
            Log.error(err_msg)

            self.notify_task_failed(err_msg)
        finally:
            if not self._failed:
                self._publish_completed()
            else:
                self._publish_error()
            self._notify_task_closed()
            self._cmd_handler.notify_task_closed()

    def _check_close_requested(self):
        if self.is_close_requested():
            raise CloseRequestException(f'Interrupted by {self.input().username()}')

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
        self._channel.basic_consume(queue=self.get_queue_name(), on_message_callback=self._callback, auto_ack=False)
        self._channel.start_consuming()


class GeneratorAdapter(RPCConsumer):

    _heartbit_timeout = 0

    def __init_subclass__(cls, generator_class: type(Generator), raise_on_close_request: bool, heartbit_timeout: int):

        require(issubclass(generator_class, Generator))

        cls._generator = generator_class
        cls._heartbit_timeout = heartbit_timeout
        cls._raise_on_close_request = raise_on_close_request

        super(GeneratorAdapter, cls).__init_subclass__()

    def _run_task(self):

        generator = self._generator(
            create_client_notifier(self.input().username(), self._generator.process_name()),
            create_db_handler(), self)
        generator._run()

    @classmethod
    def heartbit_timeout(cls):
        return cls._heartbit_timeout

    @classmethod
    def validate_input(cls, data: Any) -> Tuple[bool, str]:
        return cls._generator.validate_input(data)






