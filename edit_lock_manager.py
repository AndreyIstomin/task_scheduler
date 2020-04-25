from abc import ABC, abstractmethod
from PluginEngine.quadtree import QCell


class EditLockManagerInterface(ABC):

    @abstractmethod
    def sync(self):
        pass

    @abstractmethod
    def get_affected_cells(self, obj_type=None, obj_subtype=None) -> 'AffectedCells object':
        pass

    @abstractmethod
    def get_affected_objects(self, obj_type):
        pass


class AffectedCells:

    def __init__(self, manager: EditLockManagerInterface):
        self.__man = manager

        self.__cells = []
        # TODO

    def set_completed(self):
        pass

    def set_failed(self):
        pass

    def __iter__(self):
        return self.__cells.__iter__()


class AffectedObjects:

    def __init__(self, manager: EditLockManagerInterface):
        self.__man = manager

        self.__cells = []
        # TODO

    def set_completed(self):
        pass

    def set_failed(self):
        pass

    def __iter__(self):
        return self.__cells.__iter__()


class EditLockManager(EditLockManagerInterface):

    def __init__(self):

        self._id = 0

    def sync(self):
        pass

    def get_affected_cells(self, obj_type=None, obj_subtype=None) -> AffectedCells:

        return AffectedCells(self)

    def get_affected_objects(self, obj_type) -> AffectedObjects:

        return AffectedObjects(self)

