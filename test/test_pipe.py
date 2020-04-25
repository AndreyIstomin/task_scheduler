import uuid
import time
from multiprocessing import Process, Pipe
from multiprocessing.connection import Connection


def worker(conn: Connection):

    while True:

        conn.send(str(uuid.uuid4()))

        try:
            rsp = conn.recv()
            print(f'Server reply: {rsp}')

            if rsp == 'STOP':
                break

        except EOFError as err:

            print(f'Connection has been closed by host: {err}')
            break


def server():

    conn_1, conn_2 = Pipe()

    proc = Process(target=worker, args=(conn_2,))

    proc.start()

    step_count = 0
    last_step = False

    while not last_step:

        if conn_1.poll(timeout=0.002):

            last_step = step_count == 9

            rsp = uuid.UUID(conn_1.recv())
            conn_1.send('OK' if not last_step else 'STOP')
            print(f'Client uuid: {rsp}')
            step_count += 1

        time.sleep(0.02)

    proc.join()
    print('Worked finished')


if __name__ == '__main__':

    server()







