import unittest
import uuid
import time
from ctypes import c_char
from multiprocessing import Process, Value, Array
from backend.task_scheduler_service.common import array_to_uuid, uuid_to_array

empty_uuid_b = b'\x00'*16
empty_uuid = uuid.UUID(bytes=empty_uuid_b)


class SharedMemoryTestCase(unittest.TestCase):

    @staticmethod
    def run_process(value: Array, stop_flag: Value, _uuid: uuid.UUID):

        uuid_to_array(value, _uuid)

        while not stop_flag.value:
            time.sleep(0.1)

        uuid_to_array(value, empty_uuid)

    def test_array_uuid_conversion(self):

        uuid_holder = Array('B', empty_uuid_b)
        stop_flag = Value('i', 0)

        _uuid = uuid.uuid4()

        expected_result = [empty_uuid, _uuid, empty_uuid]
        result = []

        p = Process(target=self.run_process, args=(uuid_holder, stop_flag, _uuid))

        result.append(array_to_uuid(uuid_holder))

        p.start()

        time.sleep(1)

        result.append(array_to_uuid(uuid_holder))

        if array_to_uuid(uuid_holder) == _uuid:
            stop_flag.value = 1

        p.join()

        result.append(array_to_uuid(uuid_holder))

        self.assertEqual(result, expected_result)


if __name__ == '__main__':

    unittest.main()










