import json
import jsonschema
import os
import pprint
import xml.etree.ElementTree as ET
from PluginEngine import Log
from PluginEngine.common import require
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.road.common import IL_SUBTYPE
from backend.task_scheduler_service.schemas import SCENARIO_SCHEMA


class Scenario:
    def __init__(self, name: str, steps: 'list of strings'):
        self._name = name
        self._steps = steps

    def step_count(self) -> int:
        return len(self._steps)

    def name(self):
        return self._name

    def get_request(self, step: int) -> str:
        return self._steps[step]

    def __iter__(self):

        return self._steps.__iter__()


class InputType:
    CELLS = 0
    RECT = 1

    @staticmethod
    def verbose(_type: int):

        return ['cells', 'rect'][_type]


class ScenarioProvider:

    _type_name_map = {LANDSCAPE_OBJECT_TYPE.verbose(t): t for t in LANDSCAPE_OBJECT_TYPE}
    _subtype_name_map = {
        LANDSCAPE_OBJECT_TYPE.INFRASTRUCTURE_LINE: {IL_SUBTYPE.verbose(t): t for t in IL_SUBTYPE}
    }

    _input_type_map = {'cells': InputType.CELLS, 'rect': InputType.RECT}

    class ParseError(Exception):
        pass

    class Input:
        def __init__(self, input_type):
            self.type = input_type

        def __str__(self):

            return f'input: {InputType.verbose(ScenarioProvider._input_type_map[self.type])}'

    class Run:

        def __init__(self, routing_key: str):
            self.routing_key = routing_key

        def __str__(self):

            return f'run: {self.routing_key}'

    class LockCells:

        def __init__(self, obj_type: int, obj_subtypes: list):
            self.type = obj_type
            self.subtypes = obj_subtypes

        def __str__(self):
            return f'lock cells: type {LANDSCAPE_OBJECT_TYPE.verbose(self.type)} ({", ".join(map(str, self.subtypes))})'

    class UnlockCells:
        def __str__(self):
            return 'unlock cells'

    class LockObjects:

        def __init__(self, obj_types: list):
            self.types = obj_types

        def __str__(self):
            return f'lock objects: {", ".join(LANDSCAPE_OBJECT_TYPE.verbose(t) for t in self.types)}'

    class UnlockObjects:
        def __str__(self):
            return 'unlock objects'

    def __init__(self, path:str):
        self._path = path

    def get_scenario(self, task_id) -> (Scenario, str):
        """
        Temp implementation
        """

        # steps = ['import_road_osm', 'generate_road']
        # steps = ['consumer_A', 'consumer_B']
        # steps = ['consumer_A']
        # steps = ['road_generator']
        # name = 'test_scenario'
        # json_data = json.dumps(steps)

        steps = []

        tree = ET.parse(self._path)
        root = tree.getroot()
        if root.tag != 'scenario':
            return None, 'Scenario root tag must be "scenario"'
        if 'name' not in root.attrib:
            return None, 'tag "name" is not specified in tag "scenario"'

        name = root.attrib['name']

        try:
            for child in root:
                self._parse_tag(child, steps)
        except ScenarioProvider.ParseError as err:
            return None, str(err)

        return Scenario(name, steps), 'OK'

        # try:
        #     steps = json.loads(json_data)
        #     jsonschema.validate(steps, SCENARIO_SCHEMA)
        #     sc = Scenario(name, steps)
        # except json.JSONDecodeError as err:
        #     return None, 'Invalid scenario JSON'
        # except jsonschema.ValidationError as err:
        #     return None, 'Incorrect scenario JSON format'

        # if sc.step_count() == 0:
        #     return None, 'Empty scenario'
        #
        # return sc, 'Ok'

    def _parse_tag(self, elem: ET.Element, steps: list):

        if elem.tag == 'input':
            if 'type' not in elem.attrib:
                raise(ScenarioProvider.ParseError('attribute "type" is not specified in tag "input"'))

            input_type = elem.attrib['type']
            if elem.attrib['type'] not in self._input_type_map:
                raise(ScenarioProvider.ParseError(f'Unknown input type: {input_type}'))

            steps.append(ScenarioProvider.Input(input_type))

        elif elem.tag == 'run':
            steps.append(ScenarioProvider.Run(elem.text))

        elif elem.tag == 'lock_cells':
            if 'type' not in elem.attrib:
                raise(ScenarioProvider.ParseError('attribute "type" is not specified in tag "lock_cells"'))

            if elem.attrib['type'] not in self._type_name_map:
                raise (ScenarioProvider.ParseError(f'unknown landscape object type {elem.attrib[type]}'))
            obj_type = self._type_name_map[elem.attrib['type']]
            obj_subtype = []
            if 'subtype' in elem.attrib:
                try:
                    obj_subtype = [self._subtype_name_map[obj_type][text.strip(' ')]
                                   for text in elem.attrib['subtype'].split(',')]
                except KeyError:
                    raise ScenarioProvider.ParseError(f'Incorrect subtype attribute: {elem.attrib["subtype"]}')

            steps.append(ScenarioProvider.LockCells(obj_type, obj_subtype))

            for child in elem:
                self._parse_tag(child, steps)

            steps.append(ScenarioProvider.UnlockCells())

        elif elem.tag == 'lock_objects':
            if 'type' not in elem.attrib:
                raise ScenarioProvider.ParseError('attribute "type" is not specified in tag "lock_objects"')

            if elem.attrib['type'] not in self._type_name_map:
                raise ScenarioProvider.ParseError(f'Unknown landscape object type: {elem.attrib["type"]}')

            obj_types = [self._type_name_map[text.strip(' ')] for text in elem.attrib['type'].split(',')]

            steps.append(ScenarioProvider.LockObjects(obj_types))

            for child in elem:
                self._parse_tag(child, steps)

            steps.append(ScenarioProvider.UnlockObjects())

        elif elem.tag == 'parallel':
            Log.warn('Scenario tag parallel is not currently supported')

            for child in elem:
                self._parse_tag(child, steps)

        else:
            raise ScenarioProvider.ParseError(f'Unknown tag {elem.tag}')


def test_scenario_provider():

    path = os.path.join(os.path.dirname(__file__), 'test/test_scenario.xml')
    provider = ScenarioProvider(path)
    scenario, msg = provider.get_scenario(task_id=0)

    if scenario:
        print('-'*100)
        print(f'Scenario {scenario.name()}')
        print()
        for step in scenario._steps:
            print(step)
        print('-' * 100)

    else:
        print(msg)


if __name__ == '__main__':

    test_scenario_provider()


