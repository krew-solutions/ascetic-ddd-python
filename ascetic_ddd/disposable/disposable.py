import typing
from collections.abc import Awaitable, Callable

from .interfaces import IDisposable

__all__ = ("Disposable",)


class Disposable(IDisposable):
    _callback: Callable[[], Awaitable[None]]

    def __init__(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._callback = callback

    async def dispose(self) -> None:
        await self._callback()

    def __add__(self, other: IDisposable):
        return CompositeDisposable((self, other))


class CompositeDisposable(IDisposable):
    def __init__(self, delegates: typing.Iterable[IDisposable]) -> None:
        self._delegates = delegates

    async def dispose(self) -> None:
        for delegate in self._delegates:
            await delegate.dispose()

    def __add__(self, other: IDisposable):
        return CompositeDisposable((*tuple(self._delegates), other))
