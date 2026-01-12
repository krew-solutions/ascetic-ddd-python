from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class IAccessible(Generic[T], metaclass=ABCMeta):
    @property
    @abstractmethod
    def value(self) -> T:
        raise NotImplementedError
