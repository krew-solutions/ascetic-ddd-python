import typing
import datetime
from abc import ABCMeta, abstractmethod

from ascetic_ddd.observable.interfaces import IObservable

__all__ = (
    "ISession",
    "ISessionPool",
    "IAuthenticator",
)


class ISession(IObservable, typing.Protocol, metaclass=ABCMeta):
    response_time: float

    @abstractmethod
    async def atomic(self) -> typing.AsyncContextManager["ISession"]:
        raise NotImplementedError


class ISessionPool(IObservable, metaclass=ABCMeta):
    response_time: datetime.timedelta

    @abstractmethod
    def session(self) -> typing.AsyncContextManager[ISession]:
        raise NotImplementedError


class IAuthenticator(metaclass=ABCMeta):

    @abstractmethod
    async def authenticate(self, session: ISession):
        raise NotImplementedError
