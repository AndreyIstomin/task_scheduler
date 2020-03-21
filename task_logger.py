import asyncio
import json
from PluginEngine import LogLevel
from backend.task_scheduler_service import TaskData, RPCData, RPCStatus


class EventType:

    EVENT, TASK = 0, 1


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
        'steps': list(map(step_descriptor, task_data.requests))
    }


def message_descriptor(msg: str, level: LogLevel):
    return {
        'type': EventType.EVENT,
        'level': level,
        'msg': msg
    }


class TaskLogger:

    def __init__(self, app: 'aiohttp application'):
        self._app = app

    def new_task(self, task_data: TaskData):
        msg = f'new task: {task_data.task.name()}'
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



