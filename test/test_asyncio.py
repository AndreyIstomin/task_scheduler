import asyncio


async def test_future():

    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def callback_awaited_by_loop(result: str):

        if not fut.cancelled():
            fut.set_result(result)

    async def run_some_task(callback):

        await asyncio.sleep(2.0)
        callback('im alive')

    async def keep_calm_and_wait_some_task():
        print('Start waiting some task...')
        print('Got result:', await fut)

    loop.create_task(run_some_task(callback_awaited_by_loop))
    await loop.create_task(keep_calm_and_wait_some_task())


async def test_asyncio_queue():

    loop = asyncio.get_running_loop()

    async def msg_listener(queue: asyncio.Queue):

        for step in range(100):
            await asyncio.sleep(0.02)
            queue.put_nowait({'progress': float(step) * 0.01, 'status': 'in progress'})

        queue.put_nowait({'progress': float(1.0), 'status': 'completed'})

        # await queue.join()

        print('queue is closed')

    async def scenario_executor(queue: asyncio.Queue):

        queue_wait_timeout_sec = 1
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=queue_wait_timeout_sec)
            except asyncio.TimeoutError:
                print('Timeout error!')
                break

            if msg['status'] == 'in progress':
                print('current progress: ', int(msg['progress'] * 100), '%')
            elif msg['status'] == 'completed':
                print('the task is completed')
                break
            else:
                print('Unknown task status')
                break

    queue = asyncio.Queue()
    task_1 = asyncio.create_task(scenario_executor(queue))
    task_2 = asyncio.create_task(msg_listener(queue))

    await task_1
    await task_2

if __name__ == '__main__':

    # asyncio.run(test_future())
    asyncio.run(test_asyncio_queue())