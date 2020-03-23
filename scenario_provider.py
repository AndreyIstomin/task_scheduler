import json
import jsonschema
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


class ScenarioProvider:

    def __init__(self):
        pass

    def get_scenario(self, task_id) -> (Scenario, str):
        """
        Temp implementation
        """

        # steps = ['import_road_osm', 'generate_road']
        # steps = ['consumer_A', 'consumer_B']
        steps = ['road_generator']
        name = 'test_scenario'
        json_data = json.dumps(steps)

        try:
            steps = json.loads(json_data)
            jsonschema.validate(steps, SCENARIO_SCHEMA)
            sc = Scenario(name, steps)
        except json.JSONDecodeError as err:
            return None, 'Invalid scenario JSON'
        except jsonschema.ValidationError as err:
            return None, 'Incorrect scenario JSON format'

        if sc.step_count() == 0:
            return None, 'Empty scenario'

        return sc, 'Ok'
