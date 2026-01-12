from dataclasses import dataclass
from unittest import IsolatedAsyncioTestCase, mock

from ..mediator import Mediator


class IEvent:
    pass


class ICommand:
    pass


class Session:
    pass


@dataclass(frozen=True)
class SampleDomainEvent(IEvent):
    payload: int


@dataclass(frozen=True)
class SampleCommand(ICommand):
    payload: int


class MediatorTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mediator = Mediator[IEvent, ICommand, Session]()
        self.session = Session

    async def test_publish(self):
        handler = mock.AsyncMock()
        await self.mediator.subscribe(SampleDomainEvent, handler)
        event = SampleDomainEvent(2)
        await self.mediator.publish(event, self.session)
        handler.assert_called_once_with(event, self.session)

    async def test_unsubscribe(self):
        handler = mock.AsyncMock()
        handler2 = mock.AsyncMock()
        await self.mediator.subscribe(SampleDomainEvent, handler)
        await self.mediator.subscribe(SampleDomainEvent, handler2)
        await self.mediator.unsubscribe(SampleDomainEvent, handler)
        event = SampleDomainEvent(2)
        await self.mediator.publish(event, self.session)
        handler.assert_not_called()
        handler2.assert_called_once_with(event, self.session)

    async def test_disposable_event(self):
        handler = mock.AsyncMock()
        handler2 = mock.AsyncMock()
        disposable = await self.mediator.subscribe(SampleDomainEvent, handler)
        await self.mediator.subscribe(SampleDomainEvent, handler2)
        await disposable.dispose()
        event = SampleDomainEvent(2)
        await self.mediator.publish(event, self.session)
        handler.assert_not_called()
        handler2.assert_called_once_with(event, self.session)

    async def test_send(self):
        handler = mock.AsyncMock(return_value=5)
        await self.mediator.register(SampleCommand, handler)
        command = SampleCommand(2)
        result = await self.mediator.send(command)
        handler.assert_called_once_with(command)
        self.assertEqual(result, 5)

    async def test_unregister(self):
        handler = mock.AsyncMock()
        await self.mediator.register(SampleCommand, handler)
        await self.mediator.unregister(SampleCommand)
        command = SampleCommand(2)
        await self.mediator.send(command)
        handler.assert_not_called()

    async def test_disposable_command(self):
        handler = mock.AsyncMock()
        disposable = await self.mediator.register(SampleCommand, handler)
        await disposable.dispose()
        command = SampleCommand(2)
        await self.mediator.send(command)
        handler.assert_not_called()
