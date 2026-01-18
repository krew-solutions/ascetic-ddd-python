from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.seedwork.infrastructure.session import IAsyncConnection

__all__ = (
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
