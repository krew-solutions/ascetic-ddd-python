import typing
from aiohttp import ClientSession

from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.seedwork.infrastructure.session.interfaces import IAsyncConnection


__all__ = (
    "IExternalPgSession",
    "IInternalPgSession",
    "IRestSession",
)


@typing.runtime_checkable
class IExternalPgSession(ISession, typing.Protocol):

    @property
    def external_connection(self) -> IAsyncConnection:
        """For ReadModels (Queries)."""
        ...


@typing.runtime_checkable
class IInternalPgSession(ISession, typing.Protocol):

    @property
    def internal_connection(self) -> IAsyncConnection:
        """For ReadModels (Queries)."""
        ...


@typing.runtime_checkable
class IRestSession(ISession, typing.Protocol):

    @property
    def request(self) -> ClientSession:
        """For ReadModels (Queries)."""
        ...
