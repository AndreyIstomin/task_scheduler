import asyncio
from typing import *
from jsonschema import validate, ValidationError
from PluginEngine.asserts import require
from PluginEngine.quadtree import make_cell_by_raw_index
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.backend.schemas import RECT_SCHEMA
from LandscapeEditor.road.common import IL_SUBTYPE
from backend.task_scheduler_service.common import TaskInterface


__all__ = ["ScenarioProviderBase", "CellLocker", "ObjectLocker", "ExecutableNode", "Scenario", "GroupExecution",
           "Consequent", "Concurrent", "Run"]


class ScenarioProviderBase:
    class InputType:
        CELLS = 0
        RECT = 1

        @staticmethod
        def verbose(_type: int):
            return ['cells', 'rect'][_type]

    type_name_map = {LANDSCAPE_OBJECT_TYPE.verbose(t): t for t in LANDSCAPE_OBJECT_TYPE}
    subtype_name_map = {
        LANDSCAPE_OBJECT_TYPE.INFRASTRUCTURE_LINE: {IL_SUBTYPE.verbose(t): t for t in IL_SUBTYPE}
    }

    input_type_map = {'cells': InputType.CELLS, 'rect': InputType.RECT}

    class ParseError(Exception):
        pass


class ResourceLocker:

    def __init__(self):
        self._object_types = []
        self._lock = None
        self._active = False
        self._current_task = None

    @classmethod
    def from_str(cls, text: str):

        new_one = cls()
        for item in text.split(';'):
            if ':' in item:
                type_, subtypes = item.split(':')
                type_ = ScenarioProviderBase.type_name_map.get(type_.strip(' '), None)
                if type_ is None:
                    raise ScenarioProviderBase.ParseError(f'Unknown landscape object type {type_}')

                try:
                    subtypes = tuple(ScenarioProviderBase.subtype_name_map[type_][name.strip(' ')]
                                     for name in subtypes.split(','))
                except KeyError:
                    raise ScenarioProviderBase.ParseError(f'Unknown landscape object subtype: {subtypes}')

                new_one._object_types.append((type_, subtypes))
            else:
                type_ = ScenarioProviderBase.type_name_map.get(item.strip(' '), None)
                if type_ is None:
                    raise ScenarioProviderBase.ParseError(f'Unknown landscape object type {type_}')
                new_one._object_types.append((type_, None))
        return new_one

    def _add_locked_data(self):
        pass

    def _remove_locked_data(self, result: bool):
        pass

    def begin(self, task: TaskInterface):
        require(not self._active)
        self._active = True
        self._current_task = task
        self._add_locked_data()

    def end(self, result: bool):
        require(self._active)
        self._active = False
        self._remove_locked_data(result)
        self._lock = None
        self._current_task = None


class DummyLocker(ResourceLocker):
    pass


class CellLocker(ResourceLocker):
    def __str__(self):
        text = 'lock cells: '
        for type_, subtypes in self._object_types:
            t = subtypes or ['all']
            text += f'{LANDSCAPE_OBJECT_TYPE.verbose(type_)} ({", ".join(map(str, t))}), '
        return text

    def _add_locked_data(self):
        self._lock = self._current_task.task_manager().lock_manager().get_affected_cells(self._object_types)
        self._current_task.add_cells(self._lock)

    def _remove_locked_data(self, result: bool):
        self._current_task.remove_cells(self._lock)
        self._lock.unlock(result)


class ObjectLocker(ResourceLocker):
    def __str__(self):
        text = 'lock objects: '
        for type_, subtypes in self._object_types:
            text += f'{LANDSCAPE_OBJECT_TYPE.verbose(type_)} ({", ".join(map(str, subtypes))}), '
        return text

    def _add_locked_data(self):
        for obj_type, obj_subtypes in self._object_types:
            lock = self._current_task.task_manager().lock_manager().get_affected_objects(obj_type, obj_subtypes)
            if not lock.empty():
                self._current_task.add_objects(lock)
                self._locks.append(lock)

    def _remove_locked_data(self, result: bool):
        for lock in self._locks:
            self._current_task.remove_objects(lock)
            lock.unlock(result)


class ExecutableNode:

    def __init__(self):
        self._children = []

    def add_child(self, node: 'ExecutableNode'):
        self._children.append(node)

    def __iter__(self):
        return self._children.__iter__()

    def children(self):
        return self._children.__iter__()

    def _properties_str(self):
        return ''

    def _node_type_name(self):
        return self.__class__.__name__.lower()

    def __str__(self):
        name = self._node_type_name()
        node_list = ',\n'.join(str(node) for node in self._children)
        if node_list:
            return f'<{name} {self._properties_str()}>\n{node_list}\n</{name}>'
        else:
            return f'<{name} {self._properties_str()}/>'

    async def execute(self, task: TaskInterface) -> bool:
        raise NotImplementedError


class Scenario(ExecutableNode):
    def __init__(self, name: str):
        ExecutableNode.__init__(self)
        self._name = name
        self._input_type = None

    def _properties_str(self) -> str:
        return f'name="{self.name()}", input={self.input_type()}'

    def name(self) -> str:
        return self._name

    def set_input_type(self, _type: int):
        self._input_type = _type

    def input_type(self) -> Union[int, None]:
        return self._input_type

    def check_input(self, payload: Dict[str, Any]) -> (bool, str):

        if self._input_type == ScenarioProviderBase.InputType.RECT:
            try:
                validate(payload, RECT_SCHEMA)
            except ValidationError as err:
                return False, str(err)

        elif self._input_type == ScenarioProviderBase.InputType.CELLS:
            try:
                cells = list(map(make_cell_by_raw_index, payload['cells']))
            except Exception as err:
                return False, 'Task input must contain cells: \n' + str(err)
        return True, 'Ok'

    async def execute(self, task: TaskInterface):
        require(len(self._children) == 1, 'Multiple group execution nodes in root')
        for child in self:
            await child.execute(task)

        task.task_manager().notify_task_closed(task.uuid())


class GroupExecution(ExecutableNode):

    def __init__(self, locker=None):
        ExecutableNode.__init__(self)
        self._locker = locker or DummyLocker()

    def _properties_str(self):
        if self._locker and not isinstance(self._locker, DummyLocker):
            return f'locker="{self._locker}"'
        else:
            return ''

    async def execute(self, task: TaskInterface):
        raise NotImplementedError


class Consequent(GroupExecution):

    async def execute(self, task: TaskInterface):

        result = True
        try:
            self._locker.begin(task)
            for child in self:
                if not await child.execute(task):
                    result = False
                    return False
        finally:
            self._locker.end(result)
        return True


class Concurrent(GroupExecution):

    async def execute(self, task: TaskInterface):
        result = False
        try:
            self._locker.begin(task)
            threads = list(item.execute(task) for item in self)
            result = False not in await asyncio.gather(*threads)

        finally:
            self._locker.end(result)

        return result


class Run(ExecutableNode):

    def __init__(self, routing_key: str):
        ExecutableNode.__init__(self)
        self.routing_key = routing_key

    def _properties_str(self):
        return f'routing-key="{self.routing_key}"'

    async def execute(self, task: TaskInterface):
        return await task.task_manager().run_request(task.uuid(), self.routing_key)
