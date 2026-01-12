import typing
from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable

from ..disposable import IDisposable

__all__ = (
    "IUri",
    "IPayload",
    "IEventHandler",
    "IEventBus",
)

IUri = typing.TypeVar("IUri", covariant=True)
IPayload = typing.TypeVar("IPayload", covariant=True)
IEventHandler = Callable[[IUri, IPayload], Awaitable[None]]


class IEventBus(typing.Protocol[IUri, IPayload], metaclass=ABCMeta):
    @abstractmethod
    async def publish(self, uri: IUri, payload: IPayload) -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, uri: IUri, handler: IEventHandler) -> IDisposable:
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe(self, uri: IUri, handler: IEventHandler) -> None:
        raise NotImplementedError
