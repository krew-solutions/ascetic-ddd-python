"""Integration tests for Inbox with PostgreSQL."""

import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.inbox.inbox import Inbox
from ascetic_ddd.inbox.message import InboxMessage
from ascetic_ddd.utils.tests.db import make_pg_session_pool
from ascetic_ddd.utils.tests.payload import json_payload, decode_payload


class TestInbox(Inbox):
    """Concrete Inbox implementation for testing."""

    _table: str = 'inbox_test'
    _sequence: str = 'inbox_test_received_position_seq'


class InboxIntegrationTestCase(IsolatedAsyncioTestCase):
    """Integration tests for Inbox with real PostgreSQL."""

    async def asyncSetUp(self):
        self.session_pool = await make_pg_session_pool()
        self.inbox = TestInbox(self.session_pool)
        async with self.session_pool.session() as session:
            async with session.atomic():
                await self.inbox.setup(session)
        await self._truncate_table()
        self.handled_messages = []

    async def asyncTearDown(self):
        await self._drop_table()
        await self.session_pool._pool.close()

    async def _truncate_table(self):
        """Truncate inbox table before each test."""
        async with self.session_pool.session() as session:
            async with session.atomic():
                async with session.connection.cursor() as cursor:
                    await cursor.execute("TRUNCATE TABLE %s" % self.inbox._table)

    async def _drop_table(self):
        """Drop inbox table after tests."""
        async with self.session_pool.session() as session:
            async with session.atomic():
                async with session.connection.cursor() as cursor:
                    await cursor.execute("DROP TABLE IF EXISTS %s" % self.inbox._table)
                    await cursor.execute("DROP SEQUENCE IF EXISTS %s" % self.inbox._sequence)

    async def _subscriber(self, session, message: InboxMessage) -> None:
        """Test subscriber that collects messages."""
        self.handled_messages.append(message)

    async def test_publish_and_dispatch(self):
        """publish() stores message, dispatch() processes it."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"amount": 100}),
            metadata={"message_id": "uuid-123"},
        )

        await self.inbox.publish(message)
        result = await self.inbox.dispatch(self._subscriber)

        self.assertTrue(result)
        self.assertEqual(len(self.handled_messages), 1)
        self.assertEqual(self.handled_messages[0].tenant_id, "tenant1")
        self.assertEqual(self.handled_messages[0].stream_id, {"id": "order-123"})

    async def test_idempotency(self):
        """Duplicate messages are ignored."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"amount": 100}),
        )

        # Publish same message twice
        await self.inbox.publish(message)
        await self.inbox.publish(message)

        # Only one message should be processed
        await self.inbox.dispatch(self._subscriber)
        result = await self.inbox.dispatch(self._subscriber)

        self.assertFalse(result)
        self.assertEqual(len(self.handled_messages), 1)

    async def test_causal_dependencies(self):
        """Messages wait for dependencies to be processed."""
        # Message that depends on another
        dependent_message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=2,
            uri="kafka://shipments",
            payload=json_payload({"tracking": "123"}),
            metadata={
                "causal_dependencies": [
                    {
                        "tenant_id": "tenant1",
                        "stream_type": "Order",
                        "stream_id": {"id": "order-123"},
                        "stream_position": 1,
                    }
                ]
            },
        )

        # Publish dependent message first
        await self.inbox.publish(dependent_message)

        # Should not process (dependency not met)
        result = await self.inbox.dispatch(self._subscriber)
        self.assertFalse(result)

        # Now publish the dependency
        dependency_message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"amount": 100}),
        )
        await self.inbox.publish(dependency_message)

        # Process dependency first
        result = await self.inbox.dispatch(self._subscriber)
        self.assertTrue(result)
        self.assertEqual(self.handled_messages[0].uri, "kafka://orders")

        # Now dependent message can be processed
        result = await self.inbox.dispatch(self._subscriber)
        self.assertTrue(result)
        self.assertEqual(self.handled_messages[1].uri, "kafka://shipments")

    async def test_ordering_by_received_position(self):
        """Messages are processed in order of arrival."""
        for i in range(3):
            message = InboxMessage(
                tenant_id="tenant1",
                stream_type="Order",
                stream_id={"id": "order-%d" % i},
                stream_position=1,
                uri="kafka://orders",
                payload=json_payload({"type": "OrderCreated", "order": i}),
            )
            await self.inbox.publish(message)

        # Process all messages
        while await self.inbox.dispatch(self._subscriber):
            pass

        self.assertEqual(len(self.handled_messages), 3)
        for i, msg in enumerate(self.handled_messages):
            self.assertEqual(decode_payload(msg.payload)["order"], i)

    async def test_routing_by_uri(self):
        """Subscriber can route by uri."""
        handled_events = []

        async def routing_subscriber(session, message):
            if message.uri == "kafka://orders":
                handled_events.append(("orders", message.stream_id))
            elif message.uri == "kafka://shipments":
                handled_events.append(("shipments", message.stream_id))

        # Publish messages
        await self.inbox.publish(InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-1"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated"}),
        ))
        await self.inbox.publish(InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-2"},
            stream_position=1,
            uri="kafka://shipments",
            payload=json_payload({"type": "OrderShipped"}),
        ))

        # Process messages
        await self.inbox.dispatch(routing_subscriber)
        await self.inbox.dispatch(routing_subscriber)

        self.assertEqual(len(handled_events), 2)
        self.assertEqual(handled_events[0], ("orders", {"id": "order-1"}))
        self.assertEqual(handled_events[1], ("shipments", {"id": "order-2"}))

    async def test_async_iterator(self):
        """Async iterator yields messages."""
        for i in range(2):
            await self.inbox.publish(InboxMessage(
                tenant_id="tenant1",
                stream_type="Order",
                stream_id={"id": "order-%d" % i},
                stream_position=1,
                uri="kafka://orders",
                payload=json_payload({"type": "OrderCreated", "order": i}),
            ))

        messages = []
        iterator = self.inbox.__aiter__()

        # Get first message
        session, message = await iterator.__anext__()
        messages.append(message)

        # Get second message
        session, message = await iterator.__anext__()
        messages.append(message)

        await iterator.aclose()

        self.assertEqual(len(messages), 2)
        self.assertEqual(decode_payload(messages[0].payload)["order"], 0)
        self.assertEqual(decode_payload(messages[1].payload)["order"], 1)

    async def test_run_with_single_worker(self):
        """run() with single worker processes messages."""
        for i in range(3):
            await self.inbox.publish(InboxMessage(
                tenant_id="tenant1",
                stream_type="Order",
                stream_id={"id": "order-%d" % i},
                stream_position=1,
                uri="kafka://orders",
                payload=json_payload({"type": "OrderCreated", "order": i}),
            ))

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.2)
            stop_event.set()

        await asyncio.gather(
            self.inbox.run(self._subscriber, concurrency=1, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        self.assertEqual(len(self.handled_messages), 3)

    async def test_run_with_multiple_workers(self):
        """run() with multiple workers processes messages concurrently."""
        for i in range(10):
            await self.inbox.publish(InboxMessage(
                tenant_id="tenant1",
                stream_type="Order",
                stream_id={"id": "order-%d" % i},
                stream_position=1,
                uri="kafka://orders",
                payload=json_payload({"type": "OrderCreated", "order": i}),
            ))

        # Run with multiple workers and graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            self.inbox.run(self._subscriber, concurrency=3, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        # All messages should be processed (order may vary due to concurrency)
        self.assertEqual(len(self.handled_messages), 10)

    async def test_for_update_skip_locked(self):
        """Multiple workers don't process same message (FOR UPDATE SKIP LOCKED)."""
        # Publish a single message
        await self.inbox.publish(InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-1"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
        ))

        # Run with multiple workers and graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            self.inbox.run(self._subscriber, concurrency=3, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        # Message should be processed exactly once
        self.assertEqual(len(self.handled_messages), 1)

    async def test_workers_share_uris_without_gaps_or_overlap(self):
        """Every keyed URI is processed by exactly one of several workers."""
        total = 40
        num_workers = 3

        for i in range(total):
            await self.inbox.publish(InboxMessage(
                tenant_id="tenant1",
                stream_type="Order",
                stream_id={"id": "order-%d" % i},
                stream_position=1,
                uri="kafka://orders/order-%d" % i,
                payload=json_payload({"type": "OrderCreated", "order": i}),
            ))

        # The set must contain URIs with a negative hashtext(), otherwise the
        # test would pass with a partition filter that ignores the sign.
        async with self.session_pool.session() as session:
            async with session.atomic():
                async with session.connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT count(*) FROM %s WHERE hashtext(uri) < 0" % self.inbox._table
                    )
                    row = await cursor.fetchone()
        self.assertGreater(row[0], 0)

        for worker_id in range(num_workers):
            while await self.inbox.dispatch(
                self._subscriber, worker_id=worker_id, num_workers=num_workers
            ):
                pass

        handled_by_uri = {}
        for message in self.handled_messages:
            handled_by_uri[message.uri] = handled_by_uri.get(message.uri, 0) + 1

        self.assertEqual(len(handled_by_uri), total)
        self.assertEqual(set(handled_by_uri.values()), {1})


if __name__ == '__main__':
    unittest.main()
