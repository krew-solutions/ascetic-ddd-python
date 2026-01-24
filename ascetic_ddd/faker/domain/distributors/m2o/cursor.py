import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession

__all__ = ("Cursor",)

T = typing.TypeVar("T", covariant=True)


class Cursor(ICursor, typing.Generic[T]):
    _position: int | None
    _callback: typing.Callable[[ISession, T, int | None], typing.Awaitable[None]]
    _delegate: ICursor | None = None

    def __init__(
            self,
            position: int | None,
            callback: typing.Callable[[ISession, T, int | None], typing.Awaitable[None]],
            delegate: ICursor | None = None
    ):
        self._position = position
        self._callback = callback
        self._delegate = delegate

    @property
    def position(self) -> int | None:
        if self._position is not None:
            return self._position
        if self._delegate is not None:
            return self._delegate.position
        return None

    async def append(self, session: ISession, value: T):
        await self._callback(session, value, self._position)
        if self._delegate is not None:
            await self._delegate.append(session, value)
