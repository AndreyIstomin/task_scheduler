import asyncio
import uuid
from PluginEngine import LogLevel
from PluginEngine.common import empty_uuid
from backend.task_scheduler_service import TaskData, RPCData, RPCStatus, CloseRequest


class EventType:

    EVENT, TASK, CMD = range(3)


def step_descriptor(rpc_data: RPCData):

    return {
        'uuid': str(rpc_data.uuid),
        'name': rpc_data.routing_key,
        'progress': rpc_data.progress,
        'status': rpc_data.status,
        'msg': rpc_data.message
    }


def task_descriptor(task_data: TaskData):
    return {
        'type': EventType.TASK,
        'uuid': str(task_data.task.uuid()),
        'name': task_data.task.name(),
        'status': task_data.status,
        'message': task_data.message,
        'steps': list(map(step_descriptor, task_data.requests))
    }


def message_descriptor(msg: str, level: LogLevel):
    return {
        'type': EventType.EVENT,
        'level': level,
        'msg': msg
    }


def close_request_descriptor(req: CloseRequest):

    return {
        'type': EventType.CMD,
        'uuid': str(req.uuid),
        'name': f'Close task {req.task_name} ({str(req.task_uuid)[0:8]})',
        'status': req.status,
        'message': req.message,
        'steps': []
    }


class TaskLogger:

    def __init__(self, app: 'aiohttp application'):
        self._app = app

    def update_close_request(self, req: CloseRequest):

        self._send_json(close_request_descriptor(req))

    def new_task(self, task_data: TaskData):
        self._send_json(task_descriptor(task_data))

    def update_task(self, task_data: TaskData):
        self._send_json(task_descriptor(task_data))

    def warning(self, msg: str):
        self._send_json(message_descriptor(msg, LogLevel.WARN))

    def error(self, msg: str):
        self._send_json(message_descriptor(msg, LogLevel.ERROR))

    def _send_json(self, json_data):

        asyncio.get_event_loop().create_task(self._async_send_json(json_data))

    async def _async_send_json(self, json_data):
        for _ws in self._app['websockets']:
            await _ws.send_json(json_data)



