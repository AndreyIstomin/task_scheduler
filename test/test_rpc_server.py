import time
from backend.config import SERVICE_CONFIG
from backend.task_scheduler_service import RPCManager, RPCConsumer, ResponseObject


def test_rpc_server():

    manager = RPCManager(regime=RPCManager.SERVER, ampq_url=SERVICE_CONFIG['task_scheduler_service']['ampq_url'],
                         heart_bit_timeout=5)

    manager.add_consumer('consumer_A', 5)
    manager.add_consumer('consumer_B', 2)
    manager.add_consumer('road_generator', 2)

    manager.run()


if __name__ == '__main__':

    test_rpc_server()




