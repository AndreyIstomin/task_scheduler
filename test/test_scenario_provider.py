import os
import unittest
from unittest.mock import MagicMock
from PluginEngine.common import empty_uuid
from backend.task_scheduler_service.scenario_provider import ScenarioProvider
from backend.task_scheduler_service.scenario_common import *
from backend.task_scheduler_service.consumers import *


class ScenarioProviderTestCase(unittest.TestCase):

    def test_xml_parsing(self):

        pr = ScenarioProvider()

        path = os.path.join(os.path.dirname(__file__), 'test_scenario_2.xml')
        pr._get_scenario_path = MagicMock(return_value=path)
        pr.load()
        scenario, msg = pr.get_scenario(task_id=empty_uuid)

        expected = Scenario(name='test_scenario_2')
        expected.set_input_type(ScenarioProvider.InputType.RECT)
        node_0 = Consequent()
        node_1 = Concurrent()
        node_1.add_child(Run('road_osm_import'))
        node_1.add_child(Run('powerline_osm_import'))
        node_1.add_child(Run('fence_osm_import'))
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
