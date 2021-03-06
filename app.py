import os
import jsonschema
import uuid
import jinja2
import aiohttp_jinja2
import logging
from aiohttp import web
from functools import partial
from LandscapeEditor.backend.schemas import DEFAULT_SCHEMA
from LandscapeEditor.backend.config import SERVICE_CONFIG
from backend.generator_service import create_db_handler
from backend.task_scheduler_service.scenario_provider import ScenarioProvider
from backend.task_scheduler_service.task_manager import TaskManager
from backend.task_scheduler_service.task_logger import TaskLogger
from backend.task_scheduler_service.edit_lock_manager import EditLockManager
from backend.task_scheduler_service.schemas import RUN_TASK_SCHEMA
from backend.task_scheduler_service.routes import routes
from backend.task_scheduler_service.consumers import *
logging.disable(logging.INFO)


task_manager = None
the_app = None


async def run_task(request):
    try:
        data = await request.json()
        jsonschema.validate(data, RUN_TASK_SCHEMA)
    except (jsonschema.ValidationError, Exception) as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text=str(err))

    try:
        task_id = uuid.UUID(data['task_id'])
    except ValueError as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text="incorrect task id UUID")

    ok, msg = await task_manager.start_task(task_id, payload=data)

    if ok:
        return web.Response(status=web.HTTPOk.status_code, text=f"Task {data['task_id']} has been created by {data['username']}")
    else:
        return web.Response(status=web.HTTPInternalServerError.status_code, text=msg)


async def run_task_by_id(request, task_id: uuid.UUID):

    try:
        data = await request.json()
        jsonschema.validate(data, DEFAULT_SCHEMA)
    except (jsonschema.ValidationError, Exception) as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text=str(err))

    try:
        task_id = uuid.UUID(str(task_id))
    except ValueError as err:
        return web.Response(status=web.HTTPBadRequest.status_code, text="incorrect task id UUID")

    ok, msg = await task_manager.start_task(task_id, payload=data)

    if ok:
        return web.Response(status=web.HTTPOk.status_code, text=f"Task {task_id} has been created by {data['username']}")
    else:
        return web.Response(status=web.HTTPInternalServerError.status_code, text=msg)


# async def stop_task(request):
#     data = await request.json()
#
#     ok, msg = task_manager.request_stop_task()
#
#     if ok:
#         return web.Response(status=web.HTTPOk.status_code, text=f"The stop request has been sent")
#     else:
#         return web.Response(status=web.HTTPInternalServerError.status_code, text=msg)


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


def init(scenario_provider: ScenarioProvider):

    def get_id(name: str) -> uuid.UUID:
        return scenario_provider.get_task_id_by_name(name)

    base_dir = os.path.dirname(__file__)

    app = web.Application()
    app['websockets'] = []
    app['static_root_url'] = '/task_viewer'
    aiohttp_jinja2.setup(
        app, loader=jinja2.FileSystemLoader(os.path.join(base_dir, os.path.normpath('task_viewer/templates'))))

    # route part
    app.add_routes([
        web.post(SERVICE_CONFIG['task_scheduler_service']['run_task_url'], run_task),
        web.post(SERVICE_CONFIG['task_scheduler_service']['import_road_url'], partial(run_task_by_id, task_id=get_id('road_osm_import'))),
        web.post(SERVICE_CONFIG['task_scheduler_service']['import_fence_url'], partial(run_task_by_id, task_id=get_id('fence_osm_import'))),
        web.post(SERVICE_CONFIG['task_scheduler_service']['import_power_line_url'], partial(run_task_by_id, task_id=get_id('powerline_osm_import'))),
        web.post(SERVICE_CONFIG['task_scheduler_service']['import_bridge_url'], partial(run_task_by_id, task_id=get_id('bridge_osm_import')))
    ])

    for route in routes:
        app.router.add_route(route[0], route[1], route[2], name=route[3])

    app.router.add_static(prefix='/task_viewer',
                          path=os.path.join(base_dir, os.path.normpath('task_viewer/static')))
    # end route part

    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == '__main__':

    logger = None
    try:
        db_handler = create_db_handler()

        sp = ScenarioProvider()
        sp.load()

        the_app = init(sp)
        logger = TaskLogger(the_app)
        edit_lock_manager = EditLockManager(db_handler)

        task_manager = TaskManager(SERVICE_CONFIG['task_scheduler_service']['amqp_url'], sp,
                                   edit_lock_manager, logger)
        task_manager.run_in_external_ioloop(web.asyncio.get_event_loop())

        the_app['task_manager'] = task_manager
        the_app['logger'] = logger

        web.run_app(the_app,
                    host=SERVICE_CONFIG['task_scheduler_service']['IP'],
                    port=SERVICE_CONFIG['task_scheduler_service']['port'])

    finally:
        if logger:
            logger.close()
