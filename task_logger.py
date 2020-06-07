import sys
import traceback
import asyncio
import uuid
import json
from typing import *
from peewee import *
from aiohttp import web
from datetime import datetime
from time import time
from collections import deque
from PluginEngine import Log, LogLevel
from backend.config import SERVICE_CONFIG
from backend.task_scheduler_service import TaskData, RPCData, CloseRequest, shorten_uuid


db = SqliteDatabase(SERVICE_CONFIG['task_scheduler_service']['log_db'])


class Event(Model):

    username = CharField(default='')
    created = DateTimeField()
    event_type = IntegerField()
    status = IntegerField(default=0)
    json_data = TextField()

    class Meta:
        database = db


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
        'status': task_data.status(),
        'message': task_data.message,
        'username': task_data.task.username(),
        'steps': list(map(step_descriptor, task_data.requests))
    }


def message_descriptor(msg: str, log_level: int):
    return {
        'type': EventType.EVENT,
        'level': log_level,
        'msg': msg
    }


def close_request_descriptor(req: CloseRequest):

    return {
        'type': EventType.CMD,
        'uuid': str(req.uuid),
        'name': f'Close task {req.task_name} ({str(req.task_uuid)[0:8]})',
        'status': req.status,
        'message': req.message,
        'username': req.username,
        'steps': []
    }


class EventDescriptor:

    def __init__(self, created: datetime, data: Dict[str, Any], completed=False):
        self.__data = data
        self.__created = created
        self.__id = 0
        self.completed = completed

    @classmethod
    def from_row(cls, row: Event):

        new_one = cls(row.created, json.loads(row.json_data), completed=True)
        new_one.__id = row.id
        return new_one

    def type(self):
        return self.__data['type']

    def uuid(self):
        return self.__data['uuid']

    def created(self):
        return self.__created

    def update(self, data: Dict[str, Any]):
        self.__data = data

    def to_str(self):
        self.__data['created'] = str(self.__created)
        self.__data['id'] = self.__id
        return json.dumps(self.__data)

    def to_row(self) -> Dict[str, Any]:

        if self.__data['type'] == EventType.EVENT:
            return {'created': self.__created, 'event_type': self.__data['type'], 'json_data': self.to_str()}
        elif self.__data['type'] in TaskLogger.task_types:
            return {'username': self.__data['username'],  'created': self.__created, 'event_type': self.__data['type'],
                    'status': self.__data['status'], 'json_data': self.to_str()}
        assert False, 'Unknown event type ' + self.__data['type']


class TaskLogger:

    group_size = 50
    task_types = (EventType.TASK, EventType.CMD)

    def __init__(self, app: 'aiohttp application'):
        Event.create_table()
        self._app = app
        self._tasks: Dict[str, EventDescriptor] = {}
        self._events: List[EventDescriptor] = []
        self._completed_events: List[EventDescriptor] = []

    def update_close_request(self, req: CloseRequest):
        uuid_str = req.uuid
        data = close_request_descriptor(req)
        event = self._tasks.get(uuid_str)
        if not event:
            self._events.append(EventDescriptor(datetime.now(), data))
            event = self._tasks[uuid_str] = self._events[-1]
        else:
            event.update(data)
        self._try_save_log()
        self._send(event.to_str())

    def update_task(self, task_data: TaskData):
        uuid_str = str(task_data.task.uuid())
        data = task_descriptor(task_data)

        event = self._tasks.get(uuid_str)
        if not event:
            self._events.append(EventDescriptor(datetime.now(), data))
            event = self._tasks[uuid_str] = self._events[- 1]
        else:
            event.update(data)
        self._try_save_log()
        self._send(event.to_str())

    def new_task(self, task_data: TaskData):
        self.update_task(task_data)

    def warning(self, msg: str):
        self.message(msg, LogLevel.WARN)

    def error(self, msg: str):
        self.message(msg, LogLevel.ERROR)

    async def load_log(self, ws: web.WebSocketResponse, num_rows: int, less_than: Union[int, None] = None):
        """
        Sends event log from the DB to the client
        :param ws: websocket to send data
        :param num_rows: maximum number of rows to be loaded
        :param less_than: the maximum id of the loaded rows must be less than the given arg

        """
        await self._load_log_from_db(ws, num_rows, less_than)

        for event in self._completed_events:
            await ws.send_str(event.to_str())

        for event in self._events:
            await ws.send_str(event.to_str())

        await ws.send_str('ready')

    def message(self, msg: str, log_level: int):
        self._events.append(EventDescriptor(datetime.now(), message_descriptor(msg, log_level), completed=True))
        self._send(self._events[-1].to_str())
        self._try_save_log()

    def notify_task_closed(self, task_uuid: uuid.UUID):

        uuid_str = str(task_uuid)
        event = self._tasks.get(uuid_str)
        if event:
            event.completed = True
        else:
            Log.warn(f'Attempt to notify unknown task {shorten_uuid(uuid_str)} has been closed')

    def _send(self, data: str):
        asyncio.get_event_loop().create_task(self._async_send(data))

    async def _async_send(self, data: str):
        for _ws in self._app['websockets']:
            try:
                await _ws.send_str(data)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = "Socket error:\n{exc_info}".format(
                    exc_info='\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                )
                Log.error(err_msg)

    def close(self):
        self._try_save_log(forced=True)

    def _try_save_log(self, forced=False):

        new_completed = 0
        for item in self._events:
            if not item.completed:
                break
            self._completed_events.append(item)
            new_completed += 1
            if item.type() in self.task_types:
                del self._tasks[str(item.uuid())]

        if new_completed:
            self._events = self._events[new_completed:]

        if len(self._completed_events) >= self.group_size or forced:
            begin = time()
            with db.connection_context():
                Event.insert_many(item.to_row() for item in self._completed_events).execute()

            self._log_event_stat(time() - begin)
            self._completed_events.clear()

    async def _load_log_from_db(self, ws: web.WebSocketResponse, num_rows: int, less_than: int):

        events = deque()

        for row in Event.select().where(Event.id < less_than).order_by(Event.id.desc()).limit(num_rows):
            await ws.send_str(EventDescriptor.from_row(row).to_str())


    def _log_event_stat(self, save_time: float, log_level=LogLevel.TRACE):

        if Log.get_log_level() <= log_level:
            Log.log_message(log_level, f"""
------------------------------------
{len(self._completed_events)} events have been saved to db
{len(self._events)} events are active
save time: {save_time * 1000} ms 
------------------------------------
""", Log.CONSOLE)







