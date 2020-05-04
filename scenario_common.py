import asyncio
from PluginEngine.asserts import require
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
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


class CellLocker(ResourceLocker):
    def __str__(self):
        text = 'lock cells: '
        for type_, subtypes in self._object_types:
            t = subtypes or ['all']
            text += f'{LANDSCAPE_OBJECT_TYPE.verbose(type_)} ({", ".join(map(str, t))}), '
        return text


class ObjectLocker(ResourceLocker):
    def __str__(self):
        text = 'lock objects: '
        for type_, subtypes in self._object_types:
            text += f'{LANDSCAPE_OBJECT_TYPE.verbose(type_)} ({", ".join(map(str, subtypes))}), '
        return text


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

    def _name(self):
        return self.__class__.__name__.lower()

    def __str__(self):
        name = self._name()
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
        self.name = name
        self.input_type = None

    def _properties_str(self):
        return f'name="{self.name}", input={self.input_type}'

    async def execute(self, task: TaskInterface):
        require(len(self._children) == 1, 'Multiple group execution nodes in root')
        for child in self:
            await child.execute(task)

        task.task_manager().notify_task_closed(task.uuid())


class GroupExecution(ExecutableNode):

    def __init__(self, locker=None):
        ExecutableNode.__init__(self)
        self._locker = locker

    def _properties_str(self):
        if self._locker:
            return f'locker="{self._locker}"'
        else:
            return ''

    async def execute(self, task: TaskInterface):
        raise NotImplementedError


class Consequent(GroupExecution):

    async def execute(self, task: TaskInterface):
        # TODO: run locker if need
        for child in self:
            if not await child.execute(task):
                return False

        return True


class Concurrent(GroupExecution):

    async def execute(self, task: TaskInterface):
        # TODO: run locker if need
        threads = list(item.execute(task) for item in self)
        result = await asyncio.gather(*threads)

        return False not in result


class Run(ExecutableNode):

    def __init__(self, routing_key: str):
        ExecutableNode.__init__(self)
        self.routing_key = routing_key

    def _properties_str(self):
        return f'routing-key="{self.routing_key}"'

    async def execute(self, task: TaskInterface):
        return await task.task_manager().run_request(task.uuid(), self.routing_key, task.payload())