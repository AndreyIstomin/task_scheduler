"""
Static initialisation of RPC consumers
"""
import time
import pika
from LandscapeEditor.road import RoadGenerator
from backend.task_scheduler_service import RPCBase, GeneratorAdapter, ResponseObject, RPCConsumer

__all__ = ['RPCRoadGenerator', 'TestConsumerA', 'TestConsumerB', 'TestConsumerC']


@RPCBase.is_consumer('road_generator')
class RPCRoadGenerator(GeneratorAdapter, generator_class=RoadGenerator, raise_on_close_request=True):
    pass


#  Test consumers
class TestRPCConsumer(RPCConsumer):

    __step_ms = 1

    def __init_subclass__(cls, step_time_ms=1):
        cls.__step_ms = step_time_ms
        super(TestRPCConsumer, cls).__init_subclass__()

    def _run_task(self):

        self.publish_progress(0.0, f"Starting the {self.instance_id()}th test RPC consumer")
        step_count = 1000
        for step in range(step_count):

            time.sleep(self.__step_ms * 0.001)

            if (step + 1) % (step_count / 10) == 0:
                pr = (step + 1)/float(step_count)
                self.publish_progress(pr, f"Current progress {int(pr * 100.0)}%")

        self.notify_task_completed(f"The {self.instance_id()}th {self.get_routing_key()} completed the task")


@RPCBase.is_consumer('consumer_A')
class TestConsumerA(TestRPCConsumer, step_time_ms=7):
    pass


@RPCBase.is_consumer('consumer_B')
class TestConsumerB(TestRPCConsumer, step_time_ms=10):
    pass


@RPCBase.is_consumer('invalid_response')
class TestConsumerC(TestRPCConsumer):

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                               routing_key=self._properties.reply_to,
                               properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                               body=b'Hello')

