import json
from backend.task_scheduler_service import ResponseStatus, ResponseObject


def test_response_object():

    d = {'status': ResponseStatus.IN_PROGRESS, 'progress': 0.55, 'error_message': 'test'}

    response = ResponseObject.from_json(json.dumps(d))

    assert (json.loads(response.to_json()) == d)


if __name__ == '__main__':

    test_response_object()
