import os
import unittest
from unittest.mock import MagicMock
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.road.common import IL_SUBTYPE
from backend.task_scheduler_service import ScenarioProvider
from backend.task_scheduler_service.scenario_provider import *


class ScenarioProviderTestCase(unittest.TestCase):

    def test_xml_parsing(self):

        pr = ScenarioProvider()

        path = os.path.join(os.path.dirname(__file__), 'test_scenario_2.xml')
        with open(path) as f:
            xml_data = f.read()

        pr.get_xml_data = MagicMock(return_value=xml_data)

        scenario, msg = pr.get_scenario(task_id=0)

        # print(f'msg: {msg}, result:\n{scenario}')

        expected = Scenario(name='test_scenario_2')
        expected.input_type = ScenarioProvider.InputType.RECT
        node_0 = Consequent()
        node_1 = Concurrent()
        node_1.add_child(Run('road_import_osm'))
        node_1.add_child(Run('powerline_import_osm'))
        node_1.add_child(Run('fence_import_osm'))
        node_2 = Concurrent(locker=CellLocker.from_str('infrastructure_line:road, powerline, fence;tree'))
        node_2.add_child(Run('road_generator'))
        node_2.add_child(Run('powerline_generator'))
        node_2.add_child(Run('fence_generator'))
        node_0.add_child(node_1)
        node_0.add_child(node_2)
        expected.add_child(node_0)
        self.assertEqual(str(scenario), str(expected))


if __name__ == '__main__':

    unittest.main()
