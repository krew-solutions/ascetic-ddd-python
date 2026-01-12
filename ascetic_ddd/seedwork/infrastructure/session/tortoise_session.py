import typing
from contextlib import asynccontextmanager

from ...domain.observable.observable import Observable
from ...domain.session.interfaces import ISessionPool, ISession
from ..session.interfaces import (
    IPgSession, IIdentityMap, IAsyncConnection
)
from .identity_map import IdentityMap
from .pg_session import AsyncConnectionStatsDecorator
from tortoise.transactions import in_transaction
from tortoise import BaseDBAsyncClient

__all__ = (
    "TortoiseSession",
    "TortoiseSessionPool",
    "extract_connection",
    "extract_client",
)


def extract_connection(session: IPgSession) -> IAsyncConnection[typing.Any]:
    return session.connection


def extract_client(session: IPgSession) -> BaseDBAsyncClient:
    return session.client


class TortoiseSessionPool(Observable, ISessionPool):
    _connection_name: typing.Optional[str]

    def __init__(self, connection_name: typing.Optional[str] = None) -> None:
        self._connection_name = connection_name
        super().__init__()

    @asynccontextmanager
    async def session(self) -> typing.AsyncIterator[ISession]:
        """
        Приходится открывать сессию уже здесь. Технически можно захватить коннект через acquire_connection(),
        но BaseDBAsyncClient не умеет работать с захваченным коннектом, и захватит его повторно.
        Это спровоцирует двукратное использование коннектов.
        Поэтому, или нужно в методе TortoiseSession.connection() raise NotImplementedError,
        или запускать транзакцию уже на создании сессии.
        Второй вариант полностью обратно совместим.
        """
        async with in_transaction(self._connection_name) as client:
            session = self._make_session(client)
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
    def _make_session(client: BaseDBAsyncClient):
        return TortoiseSession(client)


class TortoiseSession(Observable, IPgSession):
    _client: BaseDBAsyncClient
    _parent: typing.Optional["TortoiseSession"]
    _identity_map: IIdentityMap

    def __init__(
            self,
            client: BaseDBAsyncClient,
            parent: typing.Optional["TortoiseSession"] = None
    ):
        self._client = client
        self._parent = parent
        self._identity_map = IdentityMap()
        super().__init__()

    @property
    def connection(self) -> IAsyncConnection[typing.Any]:
        return AsyncConnectionStatsDecorator(self._client._connection, self)

    @property
    def client(self) -> BaseDBAsyncClient:
        return self._client

    @property
    def identity_map(self) -> IIdentityMap:
        return self._identity_map

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with self._client._in_transaction() as transactional_client:
            session = self._make_transaction_session(transactional_client)
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

    def _make_transaction_session(self, client):
        return TortoiseTransactionSession(client, self._identity_map, self)


class TortoiseTransactionSession(TortoiseSession):

    def __init__(
            self,
            client: BaseDBAsyncClient,
            identity_map: IIdentityMap,
            parent: typing.Optional["TortoiseSession"] = None
    ):
        super().__init__(client, parent)
        self._identity_map = identity_map

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with self._client._in_transaction() as transactional_client:
            savepoint_session = self._make_savepoint_session(transactional_client)
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

    def _make_savepoint_session(self, client):
        return TortoiseTransactionSession(client, self._identity_map, self)
