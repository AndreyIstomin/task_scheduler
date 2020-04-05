class ControlledException(Exception):

    pass


class ContextManager:

    def __enter__(self):
        print('__enter__')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('__exit__')


def run():

    try:

        with ContextManager() as ctx:

            print('run')
            raise ControlledException('Im here')

    except Exception as ex:

        if isinstance(ex, ControlledException):
            print(f'Controlled exception: {ex}')
        else:
            print(f'Exception: {ex}')


if __name__ == '__main__':

    run()
