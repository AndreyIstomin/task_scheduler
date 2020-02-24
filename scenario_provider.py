import json
import jsonschema
from backend.task_scheduler_service.schemas import SCENARIO_SCHEMA


class Scenario:
    def __init__(self, steps: 'list of strings'):
        self.__steps = steps

    def step_count(self) -> int:
        return len(self.__steps)

    def get_request(self, step: int) -> str:
        return self.__steps[step]


class ScenarioProvider:

    def __init__(self):
        pass

    def get_scenario(self, task_id) -> (Scenario, str):
        """
        Temp implementation
        """

        steps = ['import_road_osm', 'generate_road']
        json_data = json.dumps(steps)

        try:
            steps = json.loads(json_data)
            jsonschema.validate(steps, SCENARIO_SCHEMA)
            sc = Scenario(steps)
        except json.JSONDecodeError as err:
            return None, 'Invalid scenario JSON'
        except jsonschema.ValidationError as err:
            return None, 'Incorrect scenario JSON format'

        if sc.step_count() == 0:
            return None, 'Empty scenario'

        return sc
