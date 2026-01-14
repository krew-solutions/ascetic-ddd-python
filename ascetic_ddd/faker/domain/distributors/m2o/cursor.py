import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor

from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = ("Cursor",)

T = typing.TypeVar("T", covariant=True)


class Cursor(ICursor, typing.Generic[T]):
    position: int | None
    _callback: typing.Callable[[ISession, T, int | None], typing.Awaitable[None]]

    def __init__(
            self, position: int | None,
            callback: typing.Callable[[ISession, T, int | None], typing.Awaitable[None]]
    ):
        self.position = position
        self._callback = callback

    async def append(self, session: ISession, value: T):
        await self._callback(session, value, self.position)
