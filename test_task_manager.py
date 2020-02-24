import unittest
from backend.task_scheduler_service import TaskManager, ScenarioProvider, Scenario


class MockScenario(Scenario):

    pass


class MockScenarioProvider(ScenarioProvider):

    def __init__(self):
        pass

    def get_scenario(self, task_id):

        return MockScenario()


class TaskManagerTestCase(unittest.TestCase):

    def setUp(self) -> None:

        self._task_manager = TaskManager(scenario_provider=MockScenarioProvider())

    def test_add_task(self):

        self._task_manager.add_task(0, {'task_id': 0, 'username': 'user'})


if __name__ == '__main__':

    unittest.main()
