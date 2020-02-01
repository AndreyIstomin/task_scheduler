# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import json
import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from backend.task_scheduler_service import ExamplePublisher


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
# logging.basicConfig(filename="d:/tmp/scheduler.log", level=logging.INFO)
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class SchedulerAsyncPublisher(ExamplePublisher):

    def run_in_external_ioloop(self, ioloop):
        self._connection = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection = self.connect(custom_ioloop=ioloop)

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()

    def publish_message(self, json_data: json):
        if self._channel is None or not self._channel.is_open:
            return

        hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}  # WTF!?
        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json',
            headers=hdrs)

        message = u'مفتاح قيمة 键 值 キー 値'
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    json_data,
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('Published message # %i', self._message_number)
