import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.seedwork.domain.session import ISession as _ISession, ISessionPool as _ISessionPool
from ascetic_ddd.faker.domain.utils.stats import Collector

__all__ = (
    "ISession",
    "ISessionPool",
    "IAuthenticator",
)


class ISession(_ISession, typing.Protocol, metaclass=ABCMeta):
    response_time: float
    stats: Collector


class ISessionPool(_ISessionPool, typing.Protocol, metaclass=ABCMeta):
    response_time: float
    stats: Collector


class IAuthenticator(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    async def authenticate(self, session: ISession):
        raise NotImplementedError
