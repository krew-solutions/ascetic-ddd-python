import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.disposable import IDisposable

__all__ = (
    "IBus",
    "IHandler",
)

SessionT = typing.TypeVar("SessionT")
MessageT = typing.TypeVar("MessageT")

SessionT_contra = typing.TypeVar("SessionT_contra", contravariant=True)
MessageT_contra = typing.TypeVar("MessageT_contra", contravariant=True)


class IHandler(typing.Protocol[SessionT_contra, MessageT_contra]):
    def __call__(self, session: SessionT_contra, uri: str, message: MessageT_contra):
        ...


class IBus(typing.Generic[SessionT], metaclass=ABCMeta):
    @abstractmethod
    async def publish(self, session: SessionT, uri: str, message: MessageT) -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, uri: str, handler: IHandler[SessionT, MessageT]) -> IDisposable:
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe(self, uri: str, handler: IHandler[SessionT, MessageT]) -> None:
        raise NotImplementedError
