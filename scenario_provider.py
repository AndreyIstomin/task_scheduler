import json
import jsonschema
import os
import pprint
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from PluginEngine import Log
from PluginEngine.common import require
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.road.common import IL_SUBTYPE
from backend.task_scheduler_service.schemas import SCENARIO_SCHEMA


__all__ = ['ScenarioProviderBase', 'Scenario', 'CellLocker', 'ObjectLocker', 'Concurrent', 'Consequent', 'Run' ]


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


class Node:

    def __init__(self):
        self._children = []

    def add_child(self, node: 'Node'):
        self._children.append(node)

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


class Scenario(Node):
    def __init__(self, name: str):
        Node.__init__(self)
        self.name = name
        self.input_type = None

    def _properties_str(self):
        return f'name="{self.name}", input={self.input_type}'


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


class GroupExecution(Node):

    def __init__(self, locker=None):
        Node.__init__(self)
        self._locker = locker

    def _properties_str(self):
        if self._locker:
            return f'locker="{self._locker}"'
        else:
            return ''


class Consequent(GroupExecution):
    pass


class Concurrent(GroupExecution):
    pass


class Run(Node):

    def __init__(self, routing_key: str):
        Node.__init__(self)
        self.routing_key = routing_key

    def _properties_str(self):
        return f'routing-key="{self.routing_key}"'


class ScenarioProvider(ScenarioProviderBase):

    def __init__(self):
        self._has_root_group_execution = False

    def get_xml_data(self, task_id: int):
        path = os.path.join(os.path.dirname(__file__), 'test/test_scenario_1.xml')
        with open(path) as f:
            xml_data = f.read()

        return xml_data

    def get_scenario(self, task_id: int) -> (Scenario, str):
        """
        Temp implementation
        """

        # steps = ['import_road_osm', 'generate_road']
        # steps = ['consumer_A', 'consumer_B']
        # steps = ['consumer_A']
        # steps = ['road_generator']
        # name = 'test_scenario'
        # json_data = json.dumps(steps)

        self._has_root_group_execution = False
        root = ET.fromstring(self.get_xml_data(task_id))
        if root.tag != 'scenario':
            return None, 'Scenario root tag must be "scenario"'
        if 'name' not in root.attrib:
            return None, 'Attribute "name" is not specified in tag "scenario"'

        scenario = Scenario(root.attrib['name'])

        try:
            for child in root:
                self._parse_tag(child, scenario)
        except ScenarioProvider.ParseError as err:
            return None, str(err)

        if scenario.input_type is None:
            return None, 'Missing tag "input"'

        return scenario, 'OK'

    def _create_locker(self, attrib: dict):

        if 'lock_cells' in attrib:
            return CellLocker.from_str(attrib['lock_cells'])
        elif 'lock_objects' in attrib:
            return ObjectLocker.from_str(attrib['lock_objects'])
        else:
            return None

    def _parse_tag(self, elem: ET.Element, parent: Node):

        if elem.tag == 'input':

            if not isinstance(parent, Scenario):
                raise self.ParseError(f'Tag "input" may only be a child of the tag "scenario"')

            if 'type' not in elem.attrib:
                raise self.ParseError('attribute "type" is not specified in tag "input"')
            input_type = elem.attrib['type']
            if elem.attrib['type'] not in self.input_type_map:
                raise self.ParseError(f'Unknown input type: {input_type}')

            parent.input_type = self.input_type_map[input_type]

        elif elem.tag in ('concurrent', 'consequent'):

            if not isinstance(parent, GroupExecution) and not isinstance(parent, Scenario):
                raise self.ParseError(f'Group execution tag may only be a child of the tag "scenario" '
                                      f'or another group execution tag')

            if isinstance(parent, Scenario):
                if not self._has_root_group_execution:
                    self._has_root_group_execution = True
                else:
                    raise self.ParseError(f'Tag "scenario" may only has one group execution child')

            new_one = Concurrent(self._create_locker(elem.attrib)) if elem.tag == 'concurrent'\
                else Consequent(self._create_locker(elem.attrib))

            for child in elem:
                self._parse_tag(child, new_one)

            parent.add_child(new_one)

        elif elem.tag == 'run':
            if not isinstance(parent, GroupExecution):
                raise ScenarioProviderBase.ParseError(f'Tag "Run" may only be a child of the group execution tag')
            parent.add_child(Run(elem.text))

        else:
            raise ScenarioProvider.ParseError(f'Unknown tag {elem.tag}')