import uuid
import json
import jsonschema
import aiohttp_jinja2
from datetime import datetime
# from aiohttp_session import get_session
from aiohttp import web
# from auth.models import User
# from chat.models import Message
# from settings import log

from PluginEngine import Log
from backend.task_scheduler_service.rpc_common import CMDType
from backend.task_scheduler_service.schemas import SOCKET_MESSAGE_SCHEMA


class ChatList(web.View):
    @aiohttp_jinja2.template('index.html')
    async def get(self):
        # message = Message(self.request)
        # messages = await message.get_messages()
        return {'messages': [
            # {'user': 'user1', 'msg': 'hello', 'time': datetime.now()},
            # {'user': 'user1', 'msg': 'world', 'time': datetime.now()}
        ] }


class WebSocket(web.View):
    async def get(self):
        ws = web.WebSocketResponse()
        await ws.prepare(self.request)

        # session = await get_session(self.request)
        # user = User(self.request.db, {'id': session.get('user')})
        # login = await user.get_login()

        for _ws in self.request.app['websockets']:
            await _ws.send_str('joined')
        self.request.app['websockets'].append(ws)

        async for msg in ws:
            if msg == 'close':
                await ws.close()
            else:
                try:
                    cmd = json.loads(msg.data)
                    jsonschema.validate(SOCKET_MESSAGE_SCHEMA, cmd)
                    if cmd['cmd'] == CMDType.CLOSE_TASK:
                        self.request.app['task_manager'].request_stop_task(task_uuid=uuid.UUID(cmd['request_id']),
                                                                           username=cmd['username'])
                    else:
                        Log.error(f'Unsupported cmd: {cmd}')
                except json.JSONDecodeError as err:
                    Log.error(f'Invalid JSON: {err}')

                except jsonschema.ValidationError as err:
                    Log.info(f'Incorrect JSON format: {err}')
                # message = Message(self.request.db)
                # result = await message.save(user=login, msg=msg.data)
                # log.debug(result)
                # for _ws in self.request.app['websockets']:
                #     await _ws.send_str('im alive!')
            # elif msg.tp == MsgType.error:
            #     Log.debug('ws connection closed with exception %s' % ws.exception())

        self.request.app['websockets'].remove(ws)
        for _ws in self.request.app['websockets']:
            _ws.send_str('disconected')
        Log.debug('websocket connection closed')

        return ws