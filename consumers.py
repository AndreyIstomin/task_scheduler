"""
Static initialisation of RPC consumers
"""
import time
import pika
from typing import *
from jsonschema import validate, ValidationError
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.road.common import IL_SUBTYPE
from LandscapeEditor.backend.schemas import DEFAULT_SCHEMA
from LandscapeEditor.road import RoadGenerator, RoadLightGenerator, RoadOSMImportGenerator, FenceGenerator, FenceOSMImportGenerator, \
    PowerlineGenerator, PowerlineOSMImportGenerator, BridgeGenerator, BridgeOSMImportGenerator, \
    RoadIndonesiaImportGenerator, PowerlineIndonesiaImportGenerator, BridgeIndonesiaImportGenerator
from backend.task_scheduler_service.rpc_common import RPCRegistry, ResponseObject
from backend.task_scheduler_service.rpc_consumer import RPCConsumer, GeneratorAdapter

__all__ = ['RPCRoadGenerator', 'RPCRoadOSMImport',
           'RPCFenceGenerator', 'RPCFenceOSMImport',
           'RPCPowerlineGenerator', 'RPCPowerlineOSMImport',
           'TestConsumerA', 'TestConsumerB', 'TestConsumerC', 'TestConsumerD']


@RPCRegistry.is_consumer('road_osm_import')
class RPCRoadOSMImport(GeneratorAdapter, generator_class=RoadOSMImportGenerator, raise_on_close_request=True,
                       heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('road_indonesia_import')
class RPCRoadIndonesiaImport(GeneratorAdapter, generator_class=RoadIndonesiaImportGenerator,
                             raise_on_close_request=True,
                             heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('road_generator')
class RPCRoadGenerator(GeneratorAdapter, generator_class=RoadGenerator, raise_on_close_request=True,
                       heartbit_timeout=600):
    pass


@RPCRegistry.is_consumer('road_light_generator')
class RPCRoadLightGenerator(GeneratorAdapter, generator_class=RoadLightGenerator, raise_on_close_request=True,
                            heartbit_timeout=600):
    pass


@RPCRegistry.is_consumer('fence_osm_import')
class RPCFenceOSMImport(GeneratorAdapter, generator_class=FenceOSMImportGenerator, raise_on_close_request=True,
                        heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('fence_generator')
class RPCFenceGenerator(GeneratorAdapter, generator_class=FenceGenerator, raise_on_close_request=True,
                        heartbit_timeout=600):
    pass


@RPCRegistry.is_consumer('powerline_osm_import')
class RPCPowerlineOSMImport(GeneratorAdapter, generator_class=PowerlineOSMImportGenerator, raise_on_close_request=True,
                            heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('powerline_indonesia_import')
class RPCPowerlineIndonesiaImport(GeneratorAdapter, generator_class=PowerlineIndonesiaImportGenerator,
                                  raise_on_close_request=True,
                                  heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('powerline_generator')
class RPCPowerlineGenerator(GeneratorAdapter, generator_class=PowerlineGenerator, raise_on_close_request=True,
                            heartbit_timeout=600):
    pass


@RPCRegistry.is_consumer('bridge_osm_import')
class RPCBridgeOSMImport(GeneratorAdapter, generator_class=BridgeOSMImportGenerator, raise_on_close_request=True,
                         heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('bridge_indonesia_import')
class RPCBridgeIndonesiaImport(GeneratorAdapter, generator_class=BridgeIndonesiaImportGenerator,
                               raise_on_close_request=True,
                               heartbit_timeout=3600):
    pass


@RPCRegistry.is_consumer('bridge_generator')
class RPCBridgeGenerator(GeneratorAdapter, generator_class=BridgeGenerator, raise_on_close_request=True,
                         heartbit_timeout=600):
    pass


#  Test consumers
class TestRPCConsumer(RPCConsumer):

    __step_ms = 1

    def __init_subclass__(cls, step_time_ms=1, raise_on_close_req=False):
        cls.__step_ms = step_time_ms
        cls._raise_on_close_request = raise_on_close_req
        super(TestRPCConsumer, cls).__init_subclass__()

    def _run_task(self):

        self.publish_progress(0.0, f"Starting the {self.instance_id()}th test RPC consumer")
        step_count = 1000

        locked_cell_count = len(self.input().cells_by_subtype(LANDSCAPE_OBJECT_TYPE.INFRASTRUCTURE_LINE,
                                                              IL_SUBTYPE.ROAD))

        for step in range(step_count):

            time.sleep(self.__step_ms * 0.001)

            if (step + 1) % (step_count / 10) == 0:
                pr = (step + 1)/float(step_count)
                self.publish_progress(pr, f"Locked cell count: {locked_cell_count}")

        self.notify_task_completed(f"The {self.instance_id()}th {self.get_routing_key()} completed the task")

    @classmethod
    def heartbit_timeout(cls):
        return 2

    @classmethod
    def validate_input(cls, data: Any) -> Tuple[bool, str]:

        try:
            validate(data, DEFAULT_SCHEMA)
            return True, 'Ok'
        except ValidationError as err:
            return False, f'Incorrect JSON format: {err}'


@RPCRegistry.is_consumer('consumer_A')
class TestConsumerA(TestRPCConsumer, step_time_ms=2, raise_on_close_req=True):
    pass


@RPCRegistry.is_consumer('consumer_B')
class TestConsumerB(TestRPCConsumer, step_time_ms=3, raise_on_close_req=True):
    pass


@RPCRegistry.is_consumer('invalid_response')
class TestConsumerC(TestRPCConsumer):

    def _publish_response(self, response: ResponseObject):

        self._ch.basic_publish(exchange=self.EXCHANGE,
                               routing_key=self._properties.reply_to,
                               properties=pika.BasicProperties(correlation_id=self._properties.correlation_id),
                               body=b'Hello')


@RPCRegistry.is_consumer('timeout_error')
class TestConsumerD(TestRPCConsumer, step_time_ms=2, raise_on_close_req=True):

    __step_ms = 2

    @classmethod
    def heartbit_timeout(cls):
        return 5

    def _run_task(self):

        self.publish_progress(0.0, f"Starting the {self.instance_id()}th test RPC consumer")
        step_count = 1000
        for step in range(step_count):

            time.sleep(self.__step_ms * 0.001)

            if step == 100:
                time.sleep(200)

            if (step + 1) % (step_count / 10) == 0:
                pr = (step + 1)/float(step_count)
                self.publish_progress(pr, f"Current progress {int(pr * 100.0)}%")

        self.notify_task_completed(f"The {self.instance_id()}th {self.get_routing_key()} completed the task")


@RPCRegistry.is_consumer('crash')
class TestConsumerE(TestRPCConsumer):

    @classmethod
    def heartbit_timeout(cls):
        return 3600

    def _callback(self, ch, method, properties, body):
        #  First of all update vars
        self._ch = ch
        self._method = method
        self._properties = properties
        self._reset_state()
        self._ch.basic_ack(delivery_tag=self._method.delivery_tag)

        if not self._cmd_handler.try_open_task(self._properties.correlation_id):
            user = 'undefined'  # TODO
            self.notify_task_failed(f'Task has been cancelled by user {user}')
            self._publish_error()
            self._cmd_handler.notify_task_closed()

        raise RuntimeError('CRASH!!!')