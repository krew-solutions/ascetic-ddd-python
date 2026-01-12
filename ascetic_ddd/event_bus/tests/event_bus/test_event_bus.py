import threading
import time
from contextlib import contextmanager
from unittest import IsolatedAsyncioTestCase, mock

from ...in_memory_event_bus import InMemoryEventBus


class InMemoryEventBusTestCase(IsolatedAsyncioTestCase):
    uri = "sb://test-uri"
    payload = {"a": 5}

    def setUp(self) -> None:
        self.event_bus = InMemoryEventBus()

    async def test_publish(self):
        handler = mock.AsyncMock()
        await self.event_bus.subscribe(self.uri, handler)
        with self._wait_for_response_until(lambda: handler.called):
            await self.event_bus.publish(self.uri, self.payload)
        handler.assert_called_once_with(self.payload)

    async def test_unsubscribe(self):
        handler = mock.AsyncMock()
        handler2 = mock.AsyncMock()
        await self.event_bus.subscribe(self.uri, handler)
        await self.event_bus.subscribe(self.uri, handler2)
        await self.event_bus.unsubscribe(self.uri, handler)
        with self._wait_for_response_until(lambda: handler2.called):
            await self.event_bus.publish(self.uri, self.payload)
        handler.assert_not_called()
        handler2.assert_called_once_with(self.payload)

    async def test_disposable_event(self):
        handler = mock.AsyncMock()
        handler2 = mock.AsyncMock()
        disposable = await self.event_bus.subscribe(self.uri, handler)
        await self.event_bus.subscribe(self.uri, handler2)
        await disposable.dispose()
        with self._wait_for_response_until(lambda: handler2.called):
            await self.event_bus.publish(self.uri, self.payload)
        handler.assert_not_called()
        handler2.assert_called_once_with(self.payload)

    @staticmethod
    def _wait_for_response_until(*checkers):
        return wait_for_response(lambda: [wait_until(checker) for checker in checkers])


@contextmanager
def wait_for_response(target, timeout=1):
    t = threading.Thread(target=target)
    t.start()
    yield t
    t.join(timeout)


def wait_until(checker, timeout=1):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        if checker():
            return True
        time.sleep(0.01)
        now = time.time()
    return False
