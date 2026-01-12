import typing
from abc import ABCMeta, abstractmethod, ABC
from collections.abc import Awaitable, Callable

from ..disposable.interfaces import IDisposable

__all__ = (
    "IMediator",
    "ICommandHandler",
    "IEventHandler",
    "IPipelineBehavior",
    "ICommandResult",
)


ISession = typing.TypeVar("ISession", covariant=True)
ICommand = typing.TypeVar("ICommand", covariant=True)
ICommandHandler = Callable[[ICommand], Awaitable[typing.Any]]
ICommandResult = typing.TypeVar("ICommandResult")


IEvent = typing.TypeVar("IEvent", covariant=True)
IEventHandler = Callable[[IEvent, ISession], Awaitable[None]]


class IPipelineBehavior(typing.Generic[ICommand, ICommandResult], ABC):
    @abstractmethod
    async def __call__(self, command: ICommand, next_: ICommandHandler[ICommand]) -> ICommandResult:
        pass


class IMediator(typing.Generic[ICommand, IEvent, ISession], metaclass=ABCMeta):
    @abstractmethod
    async def send(self, command: ICommand):
        raise NotImplementedError

    @abstractmethod
    async def register(self, command_type: type[ICommand], handler: ICommandHandler) -> IDisposable:
        raise NotImplementedError

    @abstractmethod
    async def unregister(self, command_type: type[ICommand]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish(self, event: IEvent, session: ISession) -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, event_type: type[IEvent], handler: IEventHandler, weak: bool = False) -> IDisposable:
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe(self, event_type: type[IEvent], handler: IEventHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    async def add_pipeline(self, pipeline: IPipelineBehavior[ICommand, ICommandResult]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _execute_pipelines(self, command: ICommand, handler: ICommandHandler[ICommand]) -> ICommandResult:
        raise NotImplementedError
