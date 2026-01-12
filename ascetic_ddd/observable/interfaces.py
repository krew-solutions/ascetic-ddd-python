import typing
from collections.abc import Callable, Hashable
from abc import ABCMeta, abstractmethod

from ..disposable.interfaces import IDisposable


class IObservable(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def attach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None) -> IDisposable:
        raise NotImplementedError

    @abstractmethod
    def detach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None):
        raise NotImplementedError

    @abstractmethod
    def notify(self, aspect: Hashable, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def anotify(self, aspect: Hashable, *args, **kwargs):
        raise NotImplementedError
