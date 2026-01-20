import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.observable.interfaces import IObservable


__all__ = ('IPgRepository',)


class IPgRepository(IObservable, typing.Protocol, metaclass=ABCMeta):
    @property
    @abstractmethod
    def table(self):
        ...
