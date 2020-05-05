import uuid
from datetime import datetime
from typing import *
from dataclasses import dataclass
from PluginEngine import UseDatabase, Log, quadtree
from PluginEngine.postgis import MINIMUM_BIGINT_VALUE
from LandscapeEditor.backend import BackendDBHandler
from backend.generator_service import create_db_handler
from backend.task_scheduler_service.common import LockedData, EditLockManagerInterface


class LockedCells(LockedData):
    pass


class LockedObjects(LockedData):
    pass


@dataclass
class HistoryRow:
    id: int
    qtree_id: int
    type_id: int
    subtype_id: int
    changed: datetime
    lock_id: int
    completed: bool


class EditLockManager(EditLockManagerInterface):

    def __init__(self, db_handler: BackendDBHandler):

        self._db_handler = db_handler
        self._cell_history_id = 0
        self._lock_id = 0
        self._cell_history = []
        self._table = ['edit_history_transient']

    def sync(self):
        with UseDatabase(self._db_handler.connection_config()) as cursor:

            completed = ','.join(str(item.id) for item in self._cell_history if item.completed)
            if completed:
                _SQL = f"""DELETE FROM {self._table[0]} WHERE id in ({completed})"""
                cursor.execute(_SQL)

            cell_history = [item for item in self._cell_history if not item.completed]
            self._cell_history = cell_history

            _SQL = f"""
SELECT *, 0 as lock_id, false as completed FROM {self._table[0]} 
WHERE id > {self._cell_history_id}"""

            cursor.execute(_SQL)

            for item in cursor:
                self._cell_history.append(HistoryRow(*item))
                self._cell_history_id = max(self._cell_history[-1].id, self._cell_history_id)

        self._log_cell_history()

    def get_affected_cells(self, obj_types: list) -> LockedCells:

        self.sync()
        return self._lock_cells(obj_types)

    def get_affected_objects(self, obj_types: list) -> LockedObjects:

        self.sync()
        return LockedObjects([], lambda x: None)

    def _lock_cells(self, obj_types: List[Tuple[int, Union[List[int], None]]]) -> LockedCells:

        self._lock_id += 1
        lock_id = self._lock_id
        cells = {}
        for type_id, subtypes in obj_types:
            if subtypes:
                for subtype_id in subtypes:
                    cells[(type_id, subtype_id)] = self._lock_cells_by_type(type_id, subtype_id)
            else:
                cells[(type_id, None)] = self._lock_cells_by_type(type_id, None)

        return LockedCells(cells, lambda x: self._unlock_cells(lock_id, x))

    def _lock_cells_by_type(self, type_id: int, subtype_id: Optional[int] = None) -> List[quadtree.QCell]:
        cells = []
        if subtype_id is None:
            for row in self._cell_history:
                if not row.lock_id and not row.completed and row.type_id == type_id:
                    row.lock_id = self._lock_id
                    cells.append(quadtree.make_cell_by_raw_index(row.qtree_id + MINIMUM_BIGINT_VALUE))
        else:
            for row in self._cell_history:
                if not row.lock_id and not row.completed and (row.type_id, row.subtype_id) == (type_id, subtype_id):
                    row.lock_id = self._lock_id
                    cells.append(quadtree.make_cell_by_raw_index(row.qtree_id + MINIMUM_BIGINT_VALUE))
        return cells

    def _unlock_cells(self, lock_id: int, completed: bool):
        for item in self._cell_history:
            if item.lock_id == lock_id:
                item.lock_id = 0
                item.completed = completed
        self.sync()

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


