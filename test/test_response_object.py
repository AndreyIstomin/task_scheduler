import json
import unittest
from PluginEngine.common import empty_uuid
from backend.task_scheduler_service.common import ResponseStatus, ResponseObject


class ResponseObjectTestCase(unittest.TestCase):

    def test_response_object(self):

        d = {'status': ResponseStatus.IN_PROGRESS,
             'progress': 0.55,
             'message': 'test',
             'request_id': str(empty_uuid)}

        response = ResponseObject.from_json(json.dumps(d))

        self.assertEqual(json.loads(response.to_json()),d)


if __name__ == '__main__':

    unittest.main()
