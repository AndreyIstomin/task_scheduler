import pika
import time
from backend.task_scheduler_service import RPCBase, RPCConsumer, ResponseObject
from backend.generator_service import create_db_handler, create_client_notifier
from LandscapeEditor.road import RoadGenerator


__all__ = ["TestRPCConsumer", "TestConsumerA", "TestConsumerB", "TestConsumerC", "RPCRoadGenerator"]


@RPCBase.is_consumer('test_consumer')
class TestRPCConsumer(RPCConsumer):

    __step_ms = 1

    def __init_subclass__(cls, step_time_ms=1):
        cls.__step_ms = step_time_ms
        super(TestRPCConsumer, cls).__init_subclass__()

    def run_task(self):

        self.publish_progress(0.0, f"Starting the {self.instance_id()}th test RPC consumer")
        step_count = 1000
        for step in range(step_count):

            time.sleep(self.__step_ms * 0.001)

            if (step + 1) % (step_count / 10) == 0:
                pr = (step + 1)/float(step_count)
                self.publish_progress(pr, f"Current progress {int(pr * 100.0)}%")

        self.publish_completed(f"The {self.instance_id()}th {self.get_routing_key()} completed the task")
        self.notify_task_closed()


@RPCBase.is_consumer('consumer_A')
class TestConsumerA(TestRPCConsumer, step_time_ms=1):
    pass


@RPCBase.is_consumer('consumer_B')
class TestConsumerB(TestRPCConsumer, step_time_ms=2):
    pass


@RPCBase.is_consumer('invalid_response')
class TestConsumerC(TestRPCConsumer):

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                               routing_key=self._properties.reply_to,
                               properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                               body=b'Hello')


@RPCBase.is_consumer('road_generator')
class RPCRoadGenerator(RPCConsumer):

    def __init__(self):
        RPCConsumer.__init__(self)
        self._generator = None

    def run_task(self):
		#  self.notify_task_began() <-- TODO (idempotent)
        self._generator = RoadGenerator(create_client_notifier(self._payload['username'], RoadGenerator.process_name()),
                                      create_db_handler(), self._payload)
        self._generator.set_rpc_consumer(self)
        self._generator.run_safely()
		self.notify_task_closed()

