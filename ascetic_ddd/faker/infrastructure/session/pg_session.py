from ascetic_ddd.faker.infrastructure.session import IInternalPgSession, IExternalPgSession
from ascetic_ddd.seedwork.infrastructure.session.interfaces import IAsyncConnection
from ascetic_ddd.seedwork.infrastructure.session.identity_map import IdentityMap
from ascetic_ddd.seedwork.infrastructure.session.pg_session import PgSession, PgSessionPool, PgSavepointSession

__all__ = (
    'extract_internal_connection',
    'extract_external_connection',
    'InternalPgSessionPool',
    'InternalPgSession',
    'InternalPgTransactionSession',
    'InternalPgSavepointSession',
    'ExternalPgSessionPool',
    'ExternalPgSession',
    'ExternalPgTransactionSession',
    'ExternalPgSavepointSession',
)

from ascetic_ddd.seedwork.infrastructure.session.pg_session import PgTransactionSession


def extract_internal_connection(session: IInternalPgSession) -> IAsyncConnection:
    try:
        return session.internal_connection
    except AttributeError:
        return session.connection


def extract_external_connection(session: IExternalPgSession) -> IAsyncConnection:
    try:
        return session.external_connection
    except AttributeError:
        return session.connection


class InternalPgSessionPool(PgSessionPool):
    @staticmethod
    def _make_session(connection):
        return InternalPgSession(connection)


class InternalPgSession(PgSession):
    internal_connection = PgSession.connection

    def _make_transaction_session(self, connection):
        return InternalPgTransactionSession(connection, IdentityMap(), self)


class InternalPgTransactionSession(PgTransactionSession):
    internal_connection = PgSession.connection

    def _make_savepoint_session(self, connection):
        return InternalPgSavepointSession(connection, self._identity_map, self)


class InternalPgSavepointSession(PgSavepointSession):
    internal_connection = PgSession.connection


class ExternalPgSessionPool(PgSessionPool):
    @staticmethod
    def _make_session(connection):
        return ExternalPgSession(connection)


class ExternalPgSession(PgSession):
    external_connection = PgSession.connection

    def _make_transaction_session(self, connection):
        return ExternalPgTransactionSession(connection, IdentityMap(), self)


class ExternalPgTransactionSession(PgTransactionSession):
    external_connection = PgSession.connection

    def _make_savepoint_session(self, connection):
        return ExternalPgTransactionSession(connection, self._identity_map, self)


class ExternalPgSavepointSession(PgSavepointSession):
    external_connection = PgSession.connection
