import os
import unittest
from unittest.mock import MagicMock
from LandscapeEditor.common import LANDSCAPE_OBJECT_TYPE
from LandscapeEditor.road.common import IL_SUBTYPE
from backend.task_scheduler_service import ScenarioProvider


class ScenarioProviderTestCase(unittest.TestCase):

    def test_xml_parsing(self):

        pr = ScenarioProvider()

        path = os.path.join(os.path.dirname(__file__), 'test_scenario_1.xml')
        with open(path) as f:
            xml_data = f.read()

        pr.get_xml_data = MagicMock(return_value=xml_data)

        scenario, msg = pr.get_scenario(task_id=0)

        expected = list(map(str, [
            ScenarioProvider.Input(input_type=ScenarioProvider.InputType.RECT),
            ScenarioProvider.Run(routing_key='road_import_osm'),
            ScenarioProvider.Run(routing_key='powerline_import_osm'),
            ScenarioProvider.Run(routing_key='fence_import_osm'),
            ScenarioProvider.LockCells(obj_type=LANDSCAPE_OBJECT_TYPE.INFRASTRUCTURE_LINE,
                                       obj_subtypes=[IL_SUBTYPE.ROAD, IL_SUBTYPE.POWERLINE, IL_SUBTYPE.FENCE]),
            ScenarioProvider.Run(routing_key='road_generator'),
            ScenarioProvider.Run(routing_key='powerline_generator'),
            ScenarioProvider.Run(routing_key='fence_generator'),
            ScenarioProvider.UnlockCells()
            ]))

        self.assertEqual(list(map(str, scenario._steps)), expected)


if __name__ == '__main__':

    unittest.main()
