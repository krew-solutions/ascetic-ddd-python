import typing
from contextlib import asynccontextmanager

from .tortoise_session import extract_connection, extract_client
from ...domain.observable.observable import Observable
from ...domain.session.interfaces import ISessionPool, ISession
from ..session.interfaces import (
    IPgSession, IIdentityMap, IAsyncConnection
)
from .identity_map import IdentityMap
from .pg_session import AsyncConnectionStatsDecorator
from tortoise.transactions import in_transaction, _get_connection
from tortoise import BaseDBAsyncClient

__all__ = (
    "TortoiseSession",
    "TortoiseSessionPool",
    "extract_connection",
    "extract_client",
)


class TortoiseSessionPool(Observable, ISessionPool):
    _connection_name: typing.Optional[str]

    def __init__(self, connection_name: typing.Optional[str] = None) -> None:
        self._connection_name = connection_name
        super().__init__()

    @asynccontextmanager
    async def session(self) -> typing.AsyncIterator[ISession]:
        yield self._make_session(self._connection_name)

    @staticmethod
    def _make_session(connection_name):
        return TortoiseSession(connection_name)


class TortoiseSession(Observable, IPgSession):
    _connection_name: typing.Optional[str]

    def __init__(
            self,
            connection_name: typing.Optional[str] = None
    ):
        self._connection_name = connection_name
        self._identity_map = IdentityMap(isolation_level=IdentityMap.READ_UNCOMMITTED)
        super().__init__()

    @property
    def connection(self) -> IAsyncConnection[typing.Any]:
        raise TypeError

    @property
    def client(self) -> BaseDBAsyncClient:
        return _get_connection(self._connection_name)

    @property
    def identity_map(self) -> IIdentityMap:
        return self._identity_map

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with in_transaction(self._connection_name) as transactional_client:
            transaction_session = self._make_transaction_session(transactional_client)
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

    def _make_transaction_session(self, client):
        return TortoiseTransactionSession(client, IdentityMap(), self)


class TortoiseTransactionSession(Observable, IPgSession):
    _client: BaseDBAsyncClient
    _parent: typing.Optional["TortoiseSession"]
    _identity_map: IIdentityMap

    def __init__(
            self,
            client: BaseDBAsyncClient,
            identity_map: IIdentityMap,
            parent: typing.Optional["TortoiseSession"] = None
    ):
        self._client = client
        self._parent = parent
        self._identity_map = identity_map
        super().__init__()

    @property
    def connection(self) -> IAsyncConnection[typing.Any]:
        return AsyncConnectionStatsDecorator(self._client._connection, self)

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
