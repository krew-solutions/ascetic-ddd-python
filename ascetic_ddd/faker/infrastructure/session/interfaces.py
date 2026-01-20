import typing
from psycopg import AsyncConnection
from aiohttp import ClientSession


__all__ = (
    "IExternalPgSession",
    "IInternalPgSession",
    "IRestSession",
)

from ascetic_ddd.faker.domain.session.interfaces import ISession


@typing.runtime_checkable
class IExternalPgSession(ISession, typing.Protocol):

    @property
    def external_connection(self) -> AsyncConnection[typing.Any]:
        """For ReadModels (Queries)."""
        ...


@typing.runtime_checkable
class IInternalPgSession(ISession, typing.Protocol):

    @property
    def internal_connection(self) -> AsyncConnection[typing.Any]:
        """For ReadModels (Queries)."""
        ...


@typing.runtime_checkable
class IRestSession(ISession, typing.Protocol):

    @property
    def request(self) -> ClientSession:
        """For ReadModels (Queries)."""
        ...
