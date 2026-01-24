import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.observable.interfaces import IObservable

__all__ = (
    "ISession",
    "ISessionPool",
)


class ISession(IObservable, typing.Protocol, metaclass=ABCMeta):
    response_time: float

    @abstractmethod
    async def atomic(self) -> typing.AsyncContextManager["ISession"]:
        raise NotImplementedError


class ISessionPool(IObservable, typing.Protocol, metaclass=ABCMeta):
    response_time: float

    @abstractmethod
    def session(self) -> typing.AsyncContextManager[ISession]:
        raise NotImplementedError


class IAuthenticator(typing.Protocol):

    async def authenticate(self, session: ISession):
        ...
