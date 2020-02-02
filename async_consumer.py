# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import time
import pika
import json
from backend.task_scheduler_service import ExampleConsumer, ResponseObject, ResponseStatus

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class AsyncConsumer(ExampleConsumer):

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

        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)

        step_count = 1000
        for i in range(0, step_count):

            # some dummy work..
            time.sleep(1e-3)

            # publish progress
            channel.basic_publish(exchange='',
                                  routing_key=properties.reply_to,
                                  properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                                  body=ResponseObject(ResponseStatus.IN_PROGRESS, (i + 1) * float(step_count)).
                                  to_json().encode('ascii'))

        # publish completion
        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                              body=ResponseObject(ResponseStatus.COMPLETED, 1.0).to_json().encode('ascii'))
        self.acknowledge_message(delivery_tag=basic_deliver.delivery_tag)


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = AsyncConsumer(amqp_url)
    consumer.run()


if __name__ == '__main__':
    main()



