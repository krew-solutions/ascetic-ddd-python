import collections
import typing
from weakref import WeakSet

from .interfaces import (
    ICommandHandler,
    IEventHandler,
    IMediator,
    IPipelineBehavior,
    ICommandResult,
)
from ..disposable.interfaces import IDisposable
from ..disposable.disposable import Disposable

__all__ = ("Mediator",)

ISession = typing.TypeVar("ISession", covariant=True)
ICommand = typing.TypeVar("ICommand", covariant=True)
IEvent = typing.TypeVar("IEvent", covariant=True)


class Mediator(typing.Generic[ICommand, IEvent, ISession], IMediator[ICommand, IEvent, ISession]):
    def __init__(self) -> None:
        self._subscribers: collections.defaultdict[type[IEvent], WeakSet[IEventHandler]] = collections.defaultdict(
            WeakSet
        )
        self._weak_cache: set[IEventHandler] = set()
        self._handlers: dict[type[ICommand], ICommandHandler[ICommand]] = {}
        self._pipelines: list[IPipelineBehavior[ICommand, ICommandResult]] = []

    async def send(self, command: ICommand) -> typing.Optional[ICommandResult]:
        if handler := self._handlers.get(type(command)):
            return await self._execute_pipelines(command, handler)
        return None

    async def register(self, command_type: type[ICommand], handler: ICommandHandler[ICommand]) -> IDisposable:
        self._handlers[command_type] = handler

        async def callback():
            await self.unregister(command_type)

        return Disposable(callback)

    async def unregister(self, command_type: type[ICommand]) -> None:
        self._handlers.pop(command_type)

    async def publish(self, event: IEvent, session: ISession) -> None:
        for handler in self._subscribers[type(event)]:
            await handler(event, session)

    async def subscribe(self, event_type: type[IEvent], handler: IEventHandler, weak: bool = False) -> IDisposable:
        self._subscribers[event_type].add(handler)
        if not weak:
            self._weak_cache.add(handler)

        async def callback():
            await self.unsubscribe(event_type, handler)

        return Disposable(callback)

    async def unsubscribe(self, event_type: type[IEvent], handler: IEventHandler) -> None:
        self._subscribers[event_type].discard(handler)
        self._weak_cache.discard(handler)

    async def add_pipeline(self, pipeline: IPipelineBehavior[ICommand, ICommandResult]) -> None:
        self._pipelines.append(pipeline)

    async def _execute_pipelines(self, command: ICommand, handler: ICommandHandler[ICommand]) -> ICommandResult:
        async def next_handler(cmd: ICommand) -> ICommandResult:
            return await handler(cmd)

        current_handler = next_handler

        for pipeline in reversed(self._pipelines):
            current_handler = self._create_pipeline_handler(pipeline, current_handler)

        return await current_handler(command)

    @staticmethod
    def _create_pipeline_handler(
        pipeline: IPipelineBehavior[ICommand, ICommandResult], next_handler: ICommandHandler[ICommand]
    ) -> ICommandHandler[ICommand]:
        async def handler(cmd: ICommand) -> ICommandResult:
            return await pipeline(cmd, next_handler)

        return handler
