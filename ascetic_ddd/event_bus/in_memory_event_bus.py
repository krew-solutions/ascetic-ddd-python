import collections
import typing

from ..disposable import Disposable, IDisposable
from .interfaces import IEventBus, IEventHandler

__all__ = ("InMemoryEventBus",)

IUri = typing.TypeVar("IUri", covariant=True)
IPayload = typing.TypeVar("IPayload", covariant=True)


class InMemoryEventBus(typing.Generic[IUri, IPayload], IEventBus[IUri, IPayload]):
    def __init__(self) -> None:
        self._subscribers = collections.defaultdict(list)

    async def publish(self, uri: IUri, payload: IPayload) -> None:
        for handler in self._subscribers[uri]:
            await handler(payload)

    async def subscribe(self, uri: IUri, handler: IEventHandler) -> IDisposable:
        self._subscribers[uri].append(handler)

        async def callback() -> None:
            await self.unsubscribe(uri, handler)

        return Disposable(callback)

    async def unsubscribe(self, uri: IUri, handler: IEventHandler) -> None:
        self._subscribers[uri].remove(handler)
