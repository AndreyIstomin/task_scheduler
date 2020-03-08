import os
import jsonschema
import jinja2
import aiohttp_jinja2
from aiohttp import web
from backend.config import SERVICE_CONFIG
from backend.task_scheduler_service import ScenarioProvider, TaskManager
from backend.task_scheduler_service.schemas import RUN_TASK_SCHEMA
from backend.task_scheduler_service.routes import routes

scenario_provider = None
task_manager = None

async def run_task(request):
    data = await request.json()
    try:
        jsonschema.validate(data, RUN_TASK_SCHEMA)
    except jsonschema.ValidationError as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text=str(err))

    logger = Logger(request)
    ok, msg = task_manager.start_task(task_id=data['task_id'], payload=data)
    logger.add_msg(f'new task: {msg}')

    await logger.send()

    if ok:
        return web.Response(status=web.HTTPOk.status_code, text=f"Task {data['task_id']} has been created by {data['username']}")
    else:
        return web.Response(status=web.HTTPInternalServerError.status_code, text=msg)


async def on_shutdown(app):
    for ws in app['websockets']:
        await ws.close(code=1001, message='Server shutdown')


# async def shutdown(server, app, handler):
#
#     server.close()
#     await server.wait_closed()
#     app.client.close()  # database connection close
#     await app.shutdown()
#     await handler.finish_connections(10.0)
#     await app.cleanup()

class Logger:

    def __init__(self, request):

        self.__websockets = request.app['websockets']
        self.__msg_queue = []

    def add_msg(self, msg: str):
        self.__msg_queue.append(msg)

    async def send(self):
        for _ws in self.__websockets:
            for msg in self.__msg_queue:
                await _ws.send_str(msg)


async def init():

    base_dir = os.path.dirname(__file__)

    app = web.Application()
    app['websockets'] = []
    app['static_root_url'] = '/task_viewer'
    aiohttp_jinja2.setup(
        app, loader=jinja2.FileSystemLoader(os.path.join(base_dir, os.path.normpath('task_viewer/templates'))))

    # route part
    app.add_routes([
        web.post('/run_task', run_task)
    ])

    for route in routes:
        app.router.add_route(route[0], route[1], route[2], name=route[3])

    app.router.add_static(prefix='/task_viewer',
                          path=os.path.join(base_dir, os.path.normpath('task_viewer/static')))
    # end route part

    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == '__main__':

    app = init()

    scenario_provider = ScenarioProvider()
    task_manager = TaskManager(SERVICE_CONFIG['task_scheduler_service']['ampq_url'], scenario_provider)
    task_manager.run_in_external_ioloop(web.asyncio.get_event_loop())
    web.run_app(app,
                host=SERVICE_CONFIG['task_scheduler_service']['IP'],
                port=SERVICE_CONFIG['task_scheduler_service']['port'])
