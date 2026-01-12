import typing
from contextlib import asynccontextmanager
from psycopg import IsolationLevel

from ...domain.utils.stats import Collector
from ....observable.observable import Observable
from ....seedwork.domain.session.interfaces import ISessionPool, ISession
from ....seedwork.infrastructure.session import (
    IAsyncConnectionPool, IAsyncConnection, AsyncConnectionStatsDecorator, IPgSession
)

__all__ = (
    "PgSession",
    "PgSessionPool",
    "extract_internal_connection",
    'extract_external_connection',
)


def extract_internal_connection(session: ISession) -> IAsyncConnection:
    try:
        return session.internal_connection
    except AttributeError:
        return session.connection


def extract_external_connection(session: ISession) -> IAsyncConnection:
    try:
        return session.external_connection
    except AttributeError:
        return session.connection


class PgSessionPool(Observable, ISessionPool):
    _pool: IAsyncConnectionPool
    response_time: float

    def __init__(self, pool: IAsyncConnectionPool) -> None:
        self._pool = pool
        self.response_time = 0.0
        self.stats = Collector()
        super().__init__()

    @asynccontextmanager
    async def session(self) -> typing.AsyncIterator[ISession]:
        async with self._pool.connection() as conn:
            await conn.set_isolation_level(IsolationLevel.READ_COMMITTED)
            session = self._make_session(conn)
            await self.anotify(
                aspect='session_started',
                session=session
            )
            try:
                yield session
            finally:
                self.response_time += session.response_time
                self.stats.update(session.stats)
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
    response_time: float

    def __init__(self, connection, parent: typing.Optional["PgSession"] = None):
        # self._connection = connection
        self._connection = AsyncConnectionStatsDecorator(connection, self)
        self._parent = parent
        self.response_time = 0.0
        self.stats = Collector()
        super().__init__()

    @property
    def connection(self) -> IAsyncConnection:
        return self._connection

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with self.connection.transaction() as transaction:
            session = self._make_transaction_session(transaction.connection)
            await self.anotify(
                aspect='session_started',
                session=session
            )
            try:
                yield session
            finally:
                self.response_time += session.response_time
                await self.anotify(
                    aspect='session_ended',
                    session=session
                )

    def _make_transaction_session(self, connection):
        return PgTransactionSession(connection, self)


# TODO: Add savepoint support
class PgTransactionSession(PgSession):
    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        session = self._make_savepoint_session(self._connection)
        await self.anotify(
            aspect='session_started',
            session=session
        )
        try:
            yield session
        finally:
            self.response_time += session.response_time
            await self.anotify(
                aspect='session_ended',
                session=session
            )

    @asynccontextmanager
    async def atomic2(self) -> typing.AsyncIterator[ISession]:
        async with self.connection.transaction() as transaction:
            session = self._make_savepoint_session(transaction.connection)
            await self.anotify(
                aspect='session_started',
                session=session
            )
            try:
                yield session
            finally:
                self.response_time += session.response_time
                await self.anotify(
                    aspect='session_ended',
                    session=session
                )

    def _make_savepoint_session(self, connection):
        return PgTransactionSession(connection, self)
