import os
import xml.etree.ElementTree as ET
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from backend.task_scheduler_service.scenario_common import *

__all__ = ['ScenarioProvider']


class ScenarioProvider(ScenarioProviderBase):

    def __init__(self):
        self._has_root_group_execution = False

    def get_xml_data(self, task_id: int):
        path = os.path.join(os.path.dirname(__file__), 'test/test_scenario_3.xml')
        with open(path) as f:
            xml_data = f.read()

        return xml_data

    def get_scenario(self, task_id: int) -> (Scenario, str):
        """
        Temp implementation
        """

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