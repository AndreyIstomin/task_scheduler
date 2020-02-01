import asyncio
import pika
import json
from aiohttp import web
from backend.task_scheduler_service import SchedulerAsyncPublisher


publisher = None


async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    task_id = 0
    json_data = json.dumps({'username': name, 'task_id': task_id})
    if isinstance(publisher, SchedulerAsyncPublisher):
        publisher.publish_message(json_data)
        return web.Response(status=200, text=f'The task {task_id} has been created by {name}')
    else:
        return web.Response(status=500)

app = web.Application()
app.add_routes([web.get('/', handle),
                web.get('/task/{name}', handle)])

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

    publisher = SchedulerAsyncPublisher('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600')
    publisher.run_in_external_ioloop(web.asyncio.get_event_loop())
    web.run_app(app, port=8181)