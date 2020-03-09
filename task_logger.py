import asyncio
import json
from backend.task_scheduler_service import TaskData, RPCData, RPCStatus


def step_descriptor(rpc_data: RPCData):

    return {
        'uuid': str(rpc_data.uuid),
        'name': rpc_data.routing_key,
        'progress': rpc_data.progress,
        'status': RPCStatus.verbose(rpc_data.status),
        'msg': rpc_data.message
    }


def task_descriptor(task_data: TaskData):
    return {
        'uuid': str(task_data.task.uuid()),
        'name': task_data.task.name(),
        'steps': list(map(step_descriptor, task_data.requests))
    }


class TaskLogger:

    def __init__(self, app: 'aiohttp application'):

        self._app = app

    def new_task(self, task_data: TaskData):

        msg = f'new task: {task_data.task.name()}'
        self._send_json(task_descriptor(task_data))

    def update_task(self, task_data: TaskData):

        self._send_json(task_descriptor(task_data))

    def _send_json(self, json_data):

        asyncio.get_event_loop().create_task(self._async_send_json(json_data))

    async def _async_send_json(self, json_data):
        for _ws in self._app['websockets']:
            await _ws.send_json(json_data)



