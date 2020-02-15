import asyncio
import pika
import json
import jsonschema
from aiohttp import web
from backend.task_scheduler_service import SchedulerAsyncPublisher, ScenarioProvider, TaskManager
from backend.task_scheduler_service.schemas import RUN_TASK_SCHEMA

publisher = None
scenario_provider = None
task_manager = None



async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    task_id = 0
    json_data = json.dumps({'username': name, 'task_id': task_id})
    if isinstance(publisher, SchedulerAsyncPublisher):
        publisher.publish_message(json_data)
        return web.Response(status=200, text=f'The task {task_id} has been created by {name}')
    else:
        return web.Response(status=500)


async def run_task(request):
    data = await request.json()

    try:
        jsonschema.validate(data, RUN_TASK_SCHEMA)
    except jsonschema.ValidationError as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text=str(err))

    if task_manager.add_task(task_id=data['task_id'], payload=data):
        return web.Response(status=web.HTTPOk.status_code, text=f"Task {data['task_id']} has been created by {data['username']}")

    else:
        return web.Response(status=web.HTTPInternalServerError.status_code)


app = web.Application()
app.add_routes([
    web.get('/', handle),
    web.get('/task/{name}', handle),
    web.post('/run_task', run_task)
])

app.add_routes([])

if __name__ == '__main__':

    # import sys
    #
    # if sys.platform == "win32":
    #     from asyncio.windows_events import ProactorEventLoop
    #
    #     loop = ProactorEventLoop()
    #     asyncio.set_event_loop(loop)
    # else:
    #     loop = asyncio.SelectorEventLoop()
    #     asyncio.set_event_loop(loop)

    scenario_provider = ScenarioProvider()
    task_manager = TaskManager(scenario_provider)

    publisher = SchedulerAsyncPublisher('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600',
                                        task_manager)
    publisher.run_in_external_ioloop(web.asyncio.get_event_loop())
    web.run_app(app, port=8181)
