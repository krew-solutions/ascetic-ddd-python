import typing
from abc import ABCMeta, abstractmethod
from psycopg import AsyncConnection
from aiohttp import ClientSession


__all__ = (
    "IExternalPgSession",
    "IInternalPgSession",
    "IRestSession",
)

from ascetic_ddd.faker.domain.session.interfaces import ISession


@typing.runtime_checkable
class IExternalPgSession(ISession, typing.Protocol, metaclass=ABCMeta):

    @property
    @abstractmethod
    def external_connection(self) -> AsyncConnection[typing.Any]:
        """For ReadModels (Queries)."""
        raise NotImplementedError


@typing.runtime_checkable
class IInternalPgSession(ISession, typing.Protocol, metaclass=ABCMeta):

    @property
    @abstractmethod
    def internal_connection(self) -> AsyncConnection[typing.Any]:
        """For ReadModels (Queries)."""
        raise NotImplementedError


@typing.runtime_checkable
class IRestSession(ISession, typing.Protocol, metaclass=ABCMeta):

    @property
    @abstractmethod
    def request(self) -> ClientSession:
        """For ReadModels (Queries)."""
        raise NotImplementedError
