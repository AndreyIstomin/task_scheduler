from datetime import datetime
from typing import *
from PluginEngine import UseDatabase, Log, quadtree
from PluginEngine.postgis import MINIMUM_BIGINT_VALUE
from LandscapeEditor.backend import BackendDBHandler
from backend.generator_service import create_db_handler
from backend.task_scheduler_service.common import LockedData, EditLockManagerInterface, TypeList


class LockedCells(LockedData):
    pass


class LockedObjects(LockedData):
    pass


class HistoryRow:
    """
    TODO: dataclass should be used
    """
    def __init__(self, id: int, qtree_id: int, type_id: int, subtype_id: int, changed: datetime, lock_id: int):
        self.id = id
        self.qtree_id = qtree_id
        self.type_id = type_id
        self.subtype_id = subtype_id
        self.changed = changed
        self.lock_id = lock_id


class EditLockManager(EditLockManagerInterface):

    def __init__(self, db_handler: BackendDBHandler):

        self._db_handler = db_handler
        self._lock_id = 0
        self._cell_history = []
        self._table = ['edit_history_transient']

        with UseDatabase(self._db_handler.connection_config()) as cursor:

            _SQL = f"""UPDATE {self._table[0]} SET lock_id = 0"""
            cursor.execute(_SQL)


    def get_affected_cells(self, obj_types: list) -> LockedCells:

        return self._lock_cells(obj_types)

    def get_affected_objects(self, obj_types: list) -> LockedObjects:

        self.sync()
        return LockedObjects([], lambda x: None)

    def _lock_cells(self, obj_types: TypeList) -> LockedCells:

        self._lock_id += 1
        lock_id = self._lock_id
        cells = {}

        only_types = []
        pairs = []

        for type_id, subtypes in obj_types:
            if subtypes:
                pairs.extend(f'({type_id}, {subtype_id})' for subtype_id in subtypes)
            else:
                only_types.append(str(type_id))

        with UseDatabase(self._db_handler.connection_config()) as cursor:

            _only_types = ', '.join(only_types)
            _pairs = ', '.join(pairs)

            conditions = []
            if pairs:
                conditions.append(f'(type_id, subtype_id) in ({_pairs})')
            if only_types:
                conditions.append(f'type_id in ({_only_types})')

            cond = ' OR '.join(conditions)

            _SQL = f"""
WITH updated AS(
    UPDATE {self._table[0]} SET lock_id = {lock_id} 
    WHERE  ({cond})
    AND lock_id = 0
    RETURNING id)
SELECT * FROM {self._table[0]} WHERE id IN (SELECT id FROM updated)  
"""
            cursor.execute(_SQL)

            cells = {}
            for item in cursor:
                row = HistoryRow(*item)
                d = cells.setdefault(row.type_id, {})
                li = d.setdefault(row.subtype_id, [])
                li.append(quadtree.make_cell_by_raw_index(row.qtree_id + MINIMUM_BIGINT_VALUE))

        return LockedCells(cells, lambda x: self._unlock_cells(lock_id, x))

    def _unlock_cells(self, lock_id: int, completed: bool):

        with UseDatabase(self._db_handler.connection_config()) as cursor:

            if completed:
                _SQL = f"""DELETE FROM {self._table[0]} WHERE lock_id = {lock_id}"""

            else:
                _SQL = f"""UPDATE {self._table[0]} SET lock_id = 0 WHERE lock_id = {lock_id}"""

            cursor.execute(_SQL)


    def _log_cell_history(self, log_level=Log.TRACE):

        if Log.get_log_level() <= log_level:
            Log.log_message(log_level, log_type=Log.CONSOLE, message="""
cell history:
------------------------------------
{rows}
------------------------------------
""".format(rows='\n'.join(map(str, self._cell_history))))


if __name__ == '__main__':

    db_handler = create_db_handler()

    lock_man = EditLockManager(db_handler)
    test_table = 't_edit_history_transient'
    lock_man._table[0] = test_table

    try:
        with UseDatabase(db_handler.connection_config()) as cursor:
            _SQL = f"""CREATE TABLE {test_table} AS TABLE edit_history_transient"""
            cursor.execute(_SQL)

        lock_man.sync()

    finally:
        with UseDatabase(db_handler.connection_config()) as cursor:
            _SQL = f"""DROP TABLE IF EXISTS {test_table}"""
            cursor.execute(_SQL)


