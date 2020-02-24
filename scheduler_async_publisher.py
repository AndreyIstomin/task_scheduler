# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import json
import uuid
import pika
from PluginEngine.common import require
from backend.task_scheduler_service import ExamplePublisher, ExampleConsumer


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
# logging.basicConfig(filename="d:/tmp/scheduler.log", level=logging.INFO)
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class SchedulerAsyncFeedbackConsumer(ExampleConsumer):

    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'reply-to-queue'
    ROUTING_KEY = 'example.text'

    def __init__(self, ampq_url: str, on_message_callback):
        ExampleConsumer.__init__(self, ampq_url)
        self._on_message_callback = on_message_callback

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

        self._on_message_callback(body)

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

    def run_in_external_ioloop(self, ioloop):
        self._connection = self.connect(custom_ioloop=ioloop)


class SchedulerAsyncPublisher(ExamplePublisher):

    def __init__(self, ampq_url: str, feedback_callback):

        ExamplePublisher.__init__(self, ampq_url)

        self._feedback_consumer = None
        self._feedback_callback = feedback_callback

    REPLY_QUEUE = 'feedback_queue'

    def run_in_external_ioloop(self, ioloop):
        self._connection = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection = self.connect(custom_ioloop=ioloop)

        self._feedback_consumer = SchedulerAsyncFeedbackConsumer(self._url, self._feedback_callback)
        self._feedback_consumer.QUEUE = self.REPLY_QUEUE

        self._feedback_consumer.run_in_external_ioloop(ioloop)

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()

    def publish_message(self, routing_key: str, corr_id: uuid.UUID, payload: 'json'):

        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json',
            reply_to=self.REPLY_QUEUE,
            correlation_id=str(corr_id)
        )

        self._channel.basic_publish(self.EXCHANGE, routing_key,
                                    payload,
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('Published message # %i', self._message_number)
