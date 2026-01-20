import typing

from ascetic_ddd.seedwork.domain.session import ISession as _ISession, ISessionPool as _ISessionPool
from ascetic_ddd.faker.domain.utils.stats import Collector

__all__ = (
    "ISession",
    "ISessionPool",
    "IAuthenticator",
)


class ISession(_ISession, typing.Protocol):
    response_time: float
    stats: Collector


class ISessionPool(_ISessionPool, typing.Protocol):
    response_time: float
    stats: Collector


class IAuthenticator(typing.Protocol):

    async def authenticate(self, session: ISession):
        ...
