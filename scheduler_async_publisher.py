import logging
import json
import uuid
import pika
from backend.task_scheduler_service.asynchronous_consumer import ExampleConsumer
from backend.task_scheduler_service.asynchronous_publisher import ExamplePublisher


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger('async_publisher')
LOGGER.setLevel(logging.WARN)


class SchedulerAsyncConsumer(ExampleConsumer):



    EXCHANGE = ''
    EXCHANGE_TYPE = ''
    QUEUE = ''
    ROUTING_KEY = ''

    def __init__(self, amqp_url: str, on_message_callback):
        ExampleConsumer.__init__(self, amqp_url)
        self._on_message_callback = on_message_callback

    def channel(self):
        return self._channel

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """

        self._on_message_callback(body, properties.correlation_id)

        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message, auto_ack=True)
        self.was_consuming = True
        self._consuming = True

    def run_in_loop(self, loop):
        self._connection = self.connect(custom_ioloop=loop)


class SchedulerAsyncPublisher(ExamplePublisher):

    REPLY_QUEUE = 'reply-to-queue'
    REPLY_ROUTING_KEY = 'feedback'

    def __init__(self, amqp_url: str, feedback_callback, exchange):

        ExamplePublisher.EXCHANGE = exchange
        ExamplePublisher.EXCHANGE_TYPE = 'direct'
        ExamplePublisher.__init__(self, amqp_url)

        self._feedback_consumer = None
        self._feedback_callback = feedback_callback

    def on_exchange_declareok(self, _unused_frame, userdata):

        """Skip declaring queue"""
        LOGGER.info('Exchange declared: %s', userdata)

    def run_in_external_ioloop(self, ioloop):
        self._connection = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection = self.connect(custom_ioloop=ioloop)

        self._feedback_consumer = SchedulerAsyncConsumer(self._url, self._feedback_callback)
        self._feedback_consumer.QUEUE = self.REPLY_QUEUE
        self._feedback_consumer.EXCHANGE = self.EXCHANGE
        self._feedback_consumer.EXCHANGE_TYPE = 'direct'
        self._feedback_consumer.ROUTING_KEY = self.REPLY_ROUTING_KEY

        self._feedback_consumer.run_in_loop(ioloop)

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()

    def publish_message(self, exchange: str, routing_key: str, corr_id: uuid.UUID, payload: 'json'):

        properties = pika.BasicProperties(
            app_id='scheduler_async_publisher',
            content_type='application/json',
            reply_to=self.REPLY_ROUTING_KEY,
            correlation_id=str(corr_id)
        )

        self._channel.basic_publish(exchange, routing_key,
                                    payload,
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('Published message # %i', self._message_number)
