class ControlledException(Exception):

    pass


class ContextManager:

    def __enter__(self):
        print('__enter__')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('__exit__')


def test_1():

    try:

        with ContextManager() as ctx:

            print('run')
            raise ControlledException('Im here')

    except Exception as ex:

        if isinstance(ex, ControlledException):
            print(f'Controlled exception: {ex}')
        else:
            print(f'Exception: {ex}')


class CMDHandler:

    def __init__(self):
        pass

    def __enter__(self):

        print('__enter__')

        return None

    def __exit__(self, exc_type, exc_val, exc_tb):

        print('__exit__')

        print(f'exc_type: {exc_type}')
        print(f'exc_val: {exc_val}')
        print(f'exc_tb: {exc_tb}')

        pass


def test_2():

    handler = CMDHandler()

    with handler:

        try:

            raise Exception('exception_1')

        except Exception as ex:

            print(f'exception: {ex}')

        finally:

            print('finally')

        print('before exception 2')
        raise Exception('exception_2')
        print('after exception 2')


if __name__ == '__main__':

    # test_1()

    try:
        test_2()
    except Exception as ex:

        print(f'exception: {ex}')

    print('hello')
