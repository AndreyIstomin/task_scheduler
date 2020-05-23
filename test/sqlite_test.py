import uuid
import json
from copy import deepcopy
from time import time
from datetime import datetime
from peewee import *
from PluginEngine import LogLevel
from backend.task_scheduler_service import RPCData, TaskData, Task, RPCStatus
from backend.task_scheduler_service.task_logger import EventType, task_descriptor, message_descriptor

db = SqliteDatabase('test.db')


class Event(Model):

    username = CharField()
    created = DateTimeField()
    event_type = IntegerField()
    status = IntegerField(default=0)
    json_data = TextField()

    class Meta:
        database = db


Event.create_table()


def insert_rows():

    # with db.atomic():
    #     Event.delete().execute()

    events = []

    task = Task(uuid.uuid4(), uuid.uuid4(), {'username': 'Andrey'}, task_manager=None, lock_manager=None)
    task_data = TaskData(task)

    task_data.requests.append(RPCData(uuid.uuid4(), 'foo', 1.0, RPCStatus.COMPLETED, 'completed'))
    task_data.requests.append(RPCData(uuid.uuid4(), 'bar', 1.0, RPCStatus.FAILED, 'failed'))
    task_data.set_failed('bar error')

    task_event = {'username': task_data.task.username(),
                   'created': datetime.now(),
                   'event_type': EventType.TASK,
                   'status': task_data.status(),
                   'json_data': task_descriptor(task_data)}

    msg_event = {'username': task_data.task.username(),
                   'created': datetime.now(),
                   'event_type': EventType.EVENT,
                   'json_data': message_descriptor('test message', LogLevel.INFO)
                   }

    for _ in range(10000):

        events.append(deepcopy(task_event))

    begin = time()
    with db.connection_context():
        for idx in range(0, len(events), 100):
            # Insert 100 rows at a time.
            rows = events[idx:idx + 100]
            Event.insert_many(rows).execute()

    print(f'Insert time: {time() - begin} seconds')

    begin = time()
    with open('d:/tmp/test_log.txt', 'w+') as f:

        for event in events:
            event['created'] = str(event['created'])
            f.write(json.dumps(event))
            f.write('\n')

    print(f'Write time: {time() - begin} seconds')

    # for event in Event.select(Event):
    #     print(event.username, event.created, event.event_type, event.status, event.json_data)


if __name__ == '__main__':

    insert_rows()

