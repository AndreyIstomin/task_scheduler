import time
import json
import pika
from PluginEngine import Log
from backend.task_scheduler_service import ResponseObject, ResponseStatus
from backend.task_scheduler_service.rpc_common import RPCBase


class RPCConsumerInput:

    def __init__(self, ampq_url: str, heartbit_timeout: 'seconds', instance_id: int):

        self.ampq_url = ampq_url
        self.heartbit_timeout = heartbit_timeout
        self.instance_id = instance_id


class RPCConsumer(RPCBase):

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

    def _instance_id(self)->int:
        return self._consumer_input.instance_id

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                         routing_key=self._properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                         body=response.to_json())

    def publish_error(self, err_message: str):

        response = ResponseObject(owner=self._payload['owner_id'], request_id=self._properties.correlation_id,
                                  status=ResponseStatus.FAILED, progress=0.0, message=err_message)

        self._publish_response(response)

    def publish_progress(self, progress: '[0.0, 1.0]', message=""):

        response = ResponseObject(owner=self._payload['owner_id'], request_id=self._properties.correlation_id,
                                  status=ResponseStatus.IN_PROGRESS, progress=progress, message=message)

        self._publish_response(response)

    def publish_completed(self, message):

        response = ResponseObject(owner=self._payload['owner_id'], request_id=self._properties.correlation_id,
                                  status=ResponseStatus.COMPLETED, progress=1.0, message=message)

        self._publish_response(response)

    def notify_task_closed(self):

        if not self._task_is_closed:
            self._ch.basic_ack(delivery_tag=self._method.delivery_tag)
            self._task_is_closed = True

    def _callback(self, ch, method, properties, body):

        Log.info(f"The {self._instance_id()}th test RPC consumer got task")
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
            self.publish_error(f"Exception {err}")
        finally:
            self.notify_task_closed()

    @classmethod
    def get_routing_key(cls):
        return cls._routing_key

    def run(self, input_: RPCConsumerInput):
        self._consumer_input = input_
        params = pika.URLParameters(input_.ampq_url)
        self._connection = pika.BlockingConnection(
            params)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.CONSUMER_QUEUE)
        self._channel.queue_bind(exchange=self.EXCHANGE, queue=self.CONSUMER_QUEUE, routing_key=self.get_routing_key())
        self._channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
        self._channel.basic_consume(queue=self.CONSUMER_QUEUE, on_message_callback=self._callback, auto_ack=False)
        self._channel.start_consuming()


@RPCBase.is_consumer('test_consumer')
class TestRPCConsumer(RPCConsumer):

    def run_task(self):

        self.publish_progress(0.0, f"Starting the {self._instance_id()}th test RPC consumer")
        step_count = 1000
        for step in range(step_count):

            time.sleep(2 * 0.001)

            if (step + 1) % (step_count / 10) == 0:
                self.publish_progress((step + 1)/float(step_count), "")

        self.publish_completed(f"The {self._instance_id()}th test RPC consumer completed the task")
        self.notify_task_closed()
