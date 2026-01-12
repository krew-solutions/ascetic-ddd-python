import typing
from collections.abc import Awaitable, Callable

from ...domain.session import ISession, ISessionPool

__all__ = (
    "TransactionalCommandHandler",
    "ICommandHandlerDelegate",
)


ICommand = typing.TypeVar("ICommand", covariant=True)
ICommandHandlerDelegate = Callable[[ICommand, ISession], Awaitable[typing.Any]]


class TransactionalCommandHandler(typing.Generic[ICommand]):
    _delegate: ICommandHandlerDelegate
    _session_pool: ISessionPool

    def __init__(self, delegate: ICommandHandlerDelegate, session_pool: ISessionPool):
        self._delegate = delegate
        self._session_pool = session_pool

    async def __call__(self, command: ICommand):
        async with self._session_pool.session() as session, session.atomic() as ts_session:
            return await self._delegate(command, ts_session)
