import threading
from typing import Type, TypeVar

T = TypeVar('T')


class Singleton:
    _instance = None
    _instantiate_mutex = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instantiate_mutex.locked():
            raise ValueError(f'New object by {cls}.instantiate() instead.')

        cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def instantiate(cls: Type[T], *args, **kwargs) -> T:
        with cls._instantiate_mutex:
            self = cls.__new__(cls)
            # noinspection PyArgumentList
            self.__init__(*args, **kwargs)

        return self

    @classmethod
    def instance(cls: Type[T]) -> T:
        if cls._instance is None:
            raise ValueError('Not initialized instance.')

        assert isinstance(cls._instance, cls)
        return cls._instance
