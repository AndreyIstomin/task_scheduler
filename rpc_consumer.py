import time
import json
import pika
from PluginEngine import Log
from LandscapeEditor.backend import RPCConsumerInterface
from backend.task_scheduler_service import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase


class RPCConsumerInput:

    def __init__(self, ampq_url: str, heartbit_timeout: 'seconds', instance_id: int):

        self.ampq_url = ampq_url
        self.heartbit_timeout = heartbit_timeout
        self.instance_id = instance_id


class RPCConsumer(RPCBase, RPCConsumerInterface):

    _routing_key = None

    def run_task(self):

        raise NotImplementedError()

    def __init__(self):
        self._connection = None
        self._channel = None
        self._consumer_input = None

        self._ch = None
        self._method = None
        self._properties = None
        self._payload = None

        self._task_is_closed = False
        self._progress = 0.0
        self._message = ""

    def instance_id(self)->int:
        return self._consumer_input.instance_id

    def payload(self) -> dict:
        return self._payload

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                         routing_key=self._properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                         body=response.to_json())

    def publish_error(self, err_message: str):

        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.FAILED, progress=0.0, message=err_message)

        self._publish_response(response)

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

    def publish_completed(self, message):

        response = ResponseObject(request_id=self._properties.correlation_id,
                                  status=ResponseStatus.COMPLETED, progress=1.0, message=message)

        self._publish_response(response)

    def notify_task_closed(self):

        if not self._task_is_closed:
            self._ch.basic_ack(delivery_tag=self._method.delivery_tag)
            self._task_is_closed = True

    def _reset_state(self):
        self._task_is_closed = False
        self._progress = 0.0
        self._message = ""

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
            self.publish_error(f"Incorrect input data: {err}")
            self.notify_task_closed()
            return

        # Run task
        try:
            self.run_task()
        except Exception as err:
            Log.error(f'Consumer {self.get_routing_key()} exception: {str(err)}')
            self.publish_error(f"Exception {err}")
        finally:
            self.notify_task_closed()

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
        params = pika.URLParameters(input_.ampq_url)
        self._connection = pika.BlockingConnection(
            params)
        self._channel = self._connection.channel()
        """durable=False: RabbitMQ WILL lose the queue and messages in it if crashes or quits """
        self._channel.queue_declare(queue=self.get_queue_name(), durable=False)
        self._channel.queue_bind(exchange=self.EXCHANGE, queue=self.get_queue_name(),
                                 routing_key=self.get_routing_key())
        self._channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
        self._channel.basic_consume(queue=self.get_queue_name(), on_message_callback=self._callback, auto_ack=False)
        self._channel.start_consuming()

