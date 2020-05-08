import os
import uuid
import xml.etree.ElementTree as ET
from typing import *
from copy import deepcopy
from PluginEngine.asserts import require
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from backend.config import SERVICE_CONFIG
from backend.task_scheduler_service.scenario_common import *
from backend.task_scheduler_service.rpc_common import RPCRegistry

__all__ = ['ScenarioProvider']


class ScenarioProvider(ScenarioProviderBase):

    def __init__(self):
        self._has_root_group_execution = False
        self._names = {}
        self._notify_bindings = {}
        self._scenarios = {}

    def get_xml_data(self, task_id: int):
        path = os.path.join(os.path.dirname(__file__), 'test/test_scenario_3.xml')
        with open(path) as f:
            xml_data = f.read()

        return xml_data

    def load(self):

        self._names = {}
        self._notify_bindings = {}
        self._scenarios = {}
        path = SERVICE_CONFIG['task_scheduler_service']['scenario_db']
        root = ET.parse(path).getroot()
        if root.tag != 'config':
            raise self.ParseError('Root tag of scenario DB must be "config"')
        for child in root:
            self._load_scenario(child)

    def _load_scenario(self, node: ET.Element):
        self._has_root_group_execution = False
        if node.tag != 'scenario':
            raise self.ParseError('Scenario root tag must be "scenario"')
        if 'name' not in node.attrib:
            raise self.ParseError('Attribute "name" is not specified in tag "scenario"')
        if 'uuid' not in node.attrib:
            raise self.ParseError('Attribute "uuid" is not specified in tag "scenario"')
        try:
            task_id = uuid.UUID(node.attrib['uuid'])
        except Exception:
            raise self.ParseError('Attribute "uuid" has is incorrect value')

        if task_id in self._scenarios:
            raise self.ParseError('Duplicate scenario uuid: {}'.format(node.attrib['uuid']))
        name = node.attrib['name'].lower()
        if name in self._names:
            raise self.ParseError('Duplicate scenario name: {}'.format(name))
        if 'notify' in node.attrib and node.attrib['notify'].lower() in self._notify_bindings:
            raise self.ParseError('Duplicate notify binding: {}'.format(node.attrib['notify']))

        scenario = Scenario(name)
        for child in node:
            self._parse_tag(child, scenario)

        ok, msg = RPCRegistry.check_scenario(scenario)
        if not ok:
            raise RPCRegistry.UnknownRoutingKeyError(msg)

        self._names[scenario.name()] = task_id
        if 'notify' in node.attrib:
            self._notify_bindings[node.attrib['notify']] = task_id
        self._scenarios[task_id] = scenario

    def get_scenario(self, task_id: uuid.UUID) -> (Scenario, str):

        require(isinstance(task_id, uuid.UUID))
        if task_id in self._scenarios:
            return deepcopy(self._scenarios[task_id]), 'Ok'

        return None, f'Unknown scenario {task_id}'

    def get_task_id(self, notify: str) -> Union[uuid.UUID, None]:
        return self._notify_bindings.get(notify, None)

    def notifications(self):
        return self._notify_bindings.keys()

    @staticmethod
    def _create_locker(attrib: dict):

        if 'lock_cells' in attrib:
            return CellLocker.from_str(attrib['lock_cells'])
        elif 'lock_objects' in attrib:
            return ObjectLocker.from_str(attrib['lock_objects'])
        else:
            return None

    def _parse_tag(self, elem: ET.Element, parent: ExecutableNode):

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