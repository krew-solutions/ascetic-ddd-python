import collections
import typing

from ascetic_ddd.bus.interfaces import IBus, IHandler
from ascetic_ddd.disposable import Disposable, IDisposable

__all__ = ("InMemoryBus",)

SessionT = typing.TypeVar("SessionT")
MessageT = typing.TypeVar("MessageT")


class InMemoryBus(IBus[SessionT], typing.Generic[SessionT]):
    def __init__(self) -> None:
        self._subscribers: collections.defaultdict[str, list] = collections.defaultdict(list)

    async def publish(self, session: SessionT, uri: str, message: MessageT) -> None:
        for handler in self._subscribers[uri]:
            await handler(session, uri, message)

    async def subscribe(self, uri: str, handler: IHandler[SessionT, MessageT]) -> IDisposable:
        self._subscribers[uri].append(handler)

        async def callback() -> None:
            await self.unsubscribe(uri, handler)

        return Disposable(callback)

    async def unsubscribe(self, uri: str, handler: IHandler[SessionT, MessageT]) -> None:
        self._subscribers[uri].remove(handler)
