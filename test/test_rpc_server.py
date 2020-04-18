import sys
import logging
import argparse
from PluginEngine import Log
from backend.config import SERVICE_CONFIG
from backend.task_scheduler_service import RPCManager

# logging.getLogger('async_consumer').disabled = True
# logging.getLogger('async_publisher').disabled = True
logging.disable(level=logging.INFO)


def parse_input() -> dict:
    parser = argparse.ArgumentParser(description='Starts RPC server (a pool of specified consumers and a controller')
    parser.add_argument('--consumers', '-c', type=str, nargs='+', required=True,
                        help='list of pairs (known consumer )')
    # parser.add_argument('--log_level', '-l', required=False, type=str,
    #                     help='trace, debug, info, warning, error; default: info;')

    args = parser.parse_args()

    result = {}
    consumer_count = len(args.consumers)
    if consumer_count < 2 or consumer_count % 2 == 1:
        raise ValueError()
    for i in range(0, consumer_count, 2):
        if args.consumers[i] in result:
            raise ValueError()
        result[args.consumers[i]] = int(args.consumers[i + 1])

    # Log.set_log_level(Log.INFO)
    # if args.log_level is not None:
    #     Log_dict = {'trace': Log.TRACE, 'debug': Log.DEBUG, 'info': Log.INFO, 'warning': Log.WARN, 'error': Log.ERROR}
    #     Log.set_log_level(Log_dict[args.log_level])

    return result


def test_rpc_server():

    consumers = parse_input()
    manager = RPCManager(regime=RPCManager.SERVER, amqp_url=SERVICE_CONFIG['task_scheduler_service']['amqp_url'],
                         heart_bit_timeout=5)
    for name, count in consumers.items():
        manager.add_consumer(name, count)
    manager.run()


if __name__ == '__main__':

    test_rpc_server()




