import typing

from abc import ABCMeta, abstractmethod
from types import TracebackType

from ...domain.aggregate import IHashable
from ...domain.session.interfaces import ISession as _ISession

__all__ = (
    "Query",
    "Params",
    "Row",
    "IAsyncConnection",
    "IAsyncConnectionPool",
    "IAsyncCursor",
    "IAsyncTransaction",
    "ISession",
    "IIdentityMap",
    "IIdentityKey",
    "IModel",
    "IPgSession",
)


IIdentityKey: typing.TypeAlias = IHashable
IModel: typing.TypeAlias = typing.Any

Query: typing.TypeAlias = typing.Union[str, bytes]
Params: typing.TypeAlias = typing.Union[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]
Row = typing.TypeVar("Row", covariant=True)


@typing.runtime_checkable
class IAsyncCursor(typing.Protocol):
    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> "IAsyncCursor": ...

    async def fetchone(self) -> Row | None: ...

    async def fetchmany(self, size: int = 0) -> list[Row]: ...

    async def fetchall(self) -> list[Row]: ...

    async def close(self) -> None: ...

    async def __aenter__(self) -> "IAsyncCursor": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...


@typing.runtime_checkable
class IAsyncTransaction(typing.Protocol):
    @property
    def connection(self) -> "IAsyncConnection": ...

    async def __aenter__(self) -> "IAsyncTransaction": ...

    async def __aexit__(
        self,
        exc_type: typing.Optional[type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Any,
    ) -> None: ...


@typing.runtime_checkable
class IAsyncConnection(typing.Protocol):
    def cursor(self, *args: typing.Any, **kwargs: typing.Any) -> IAsyncCursor: ...

    def transaction(
        self,
        savepoint_name: str | None = None,
        force_rollback: bool = False
    ) -> typing.AsyncContextManager["IAsyncTransaction"]: ...

    async def close(self) -> None: ...

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool = False,
    ) -> IAsyncCursor: ...

    async def __aenter__(self) -> "IAsyncConnection": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...


class IAsyncConnectionPool(typing.Protocol):
    async def connection(self, timeout: float | None = None) -> typing.AsyncContextManager["IAsyncConnection"]: ...


class IIdentityMap(metaclass=ABCMeta):
    @abstractmethod
    def get(self, key: IIdentityKey) -> IModel | None:
        raise NotImplementedError

    @abstractmethod
    def has(self, key: IIdentityKey) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add(self, key: IIdentityKey, obj: IModel):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key: IIdentityKey) -> None:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError


class ISession(_ISession, typing.Protocol, metaclass=ABCMeta):
    @property
    @abstractmethod
    def identity_map(self) -> IIdentityMap:
        raise NotImplementedError


@typing.runtime_checkable
class IPgSession(_ISession, typing.Protocol, metaclass=ABCMeta):

    @property
    @abstractmethod
    def connection(self) -> IAsyncConnection:
        """For ReadModels (Queries)."""
        raise NotImplementedError
