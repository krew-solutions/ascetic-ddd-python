import typing
import weakref
from contextlib import asynccontextmanager
from time import perf_counter
from types import TracebackType
# from psycopg import AsyncConnection
from psycopg import IsolationLevel

from ascetic_ddd.observable.observable import Observable
from ...domain.session.interfaces import ISessionPool, ISession
from ..session.interfaces import (
    IPgSession, IIdentityMap, IAsyncConnection, IAsyncConnectionPool, IAsyncCursor, Query, Params
)
from .identity_map import IdentityMap

__all__ = (
    "PgSession",
    "PgSessionPool",
    "PgTransactionSession",
    "PgSavepointSession",
    "extract_connection",
    "AsyncCursorStatsDecorator",
    "AsyncConnectionStatsDecorator",
)


def extract_connection(session: IPgSession) -> IAsyncConnection:
    return session.connection


class PgSessionPool(Observable, ISessionPool):
    _pool: IAsyncConnectionPool

    def __init__(self, pool: IAsyncConnectionPool) -> None:
        self._pool = pool
        super().__init__()

    @asynccontextmanager
    async def session(self) -> typing.AsyncIterator[ISession]:
        async with self._pool.connection() as conn:
            # await conn.set_isolation_level(IsolationLevel.READ_COMMITTED)
            session = self._make_session(conn)
            await self.anotify(
                aspect='session_started',
                session=session
            )
            try:
                yield session
            finally:
                await self.anotify(
                    aspect='session_ended',
                    session=session
                )

    @staticmethod
    def _make_session(connection):
        return PgSession(connection)


class PgSession(Observable, IPgSession):
    _connection: IAsyncConnection
    _parent: typing.Optional["PgSession"]
    _identity_map: IIdentityMap

    def __init__(self, connection: IAsyncConnection, parent: typing.Optional["PgSession"] = None):
        # self._connection = connection
        self._connection = AsyncConnectionStatsDecorator(connection, self)
        self._parent = parent
        self._identity_map = IdentityMap(isolation_level=IdentityMap.READ_UNCOMMITTED)
        super().__init__()

    @property
    def connection(self) -> IAsyncConnection:
        return self._connection

    @property
    def identity_map(self) -> IIdentityMap:
        return self._identity_map

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with self.connection.transaction() as transaction:
            transaction_session = self._make_transaction_session(transaction.connection)
            await self.anotify(
                aspect='session_started',
                session=transaction_session
            )
            try:
                yield transaction_session
            finally:
                await self.anotify(
                    aspect='session_ended',
                    session=transaction_session
                )

    def _make_transaction_session(self, connection):
        return PgTransactionSession(connection, IdentityMap(), self)


# TODO: Add savepoint support
class PgTransactionSession(PgSession):

    def __init__(
            self,
            connection: IAsyncConnection,
            identity_map: IIdentityMap,
            parent: typing.Optional["PgSession"] = None
    ):
        super().__init__(connection, parent)
        self._identity_map = identity_map

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        savepoint_session = self._make_savepoint_session(self._connection)
        await self.anotify(
            aspect='session_started',
            session=savepoint_session
        )
        try:
            yield savepoint_session
        finally:
            await self.anotify(
                aspect='session_ended',
                session=savepoint_session
            )

    @asynccontextmanager
    async def atomic2(self) -> typing.AsyncIterator[ISession]:
        async with self.connection.transaction() as transaction:
            savepoint_session = self._make_savepoint_session(transaction.connection)
            await self.anotify(
                aspect='session_started',
                session=savepoint_session
            )
            try:
                yield savepoint_session
            finally:
                await self.anotify(
                    aspect='session_ended',
                    session=savepoint_session
                )

    def _make_savepoint_session(self, connection):
        return PgSavepointSession(connection, self._identity_map, self)


class PgSavepointSession(PgTransactionSession):
    pass


class AsyncCursorStatsDecorator:
    _delegate: IAsyncCursor
    _session: weakref.ReferenceType[IPgSession]

    def __init__(self, delegate: IAsyncCursor, session: IPgSession):
        self._delegate = delegate
        self._session = weakref.ref(session)

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ):
        await self._session().anotify(
            aspect='query_started',
            query=query,
            params=params,
            sender=self,
            session=self._session,
        )
        time_start = perf_counter()
        await self._delegate.execute(query, params, prepare=prepare, binary=binary)
        response_time = perf_counter() - time_start
        await self._session().anotify(
            aspect='query_ended',
            query=query,
            params=params,
            sender=self,
            session=self._session,
            response_time=response_time,
        )
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._delegate, name)


class AsyncConnectionStatsDecorator:
    _delegate: IAsyncConnection
    _session: weakref.ReferenceType[IPgSession]

    def __init__(self, delegate: IAsyncConnection, session: IPgSession):
        self._delegate = delegate
        self._session = weakref.ref(session)

    def cursor(self, *a, **kw):
        return AsyncCursorStatsDecorator(self._delegate.cursor(*a, **kw), self._session())

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._delegate, name)
