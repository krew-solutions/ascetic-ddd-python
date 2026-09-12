"""Integration tests for Outbox with PostgreSQL."""

import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.outbox.message import OutboxMessage
from ascetic_ddd.outbox.outbox import Outbox
from ascetic_ddd.utils.tests.db import make_pg_session_pool


class OutboxIntegrationTestCase(IsolatedAsyncioTestCase):
    """Integration tests for Outbox with real PostgreSQL."""

    _outbox_table: str = 'outbox_test'
    _offsets_table: str = 'outbox_offsets_test'

    async def asyncSetUp(self):
        self._session_pool = await make_pg_session_pool()
        self.outbox = Outbox(
            self._session_pool,
            outbox_table=self._outbox_table,
            offsets_table=self._offsets_table,
        )
        async with self._session_pool.session() as session:
            async with session.atomic():
                await self.outbox.setup(session)
        await self._truncate_tables()
        self.published_messages = []

    async def asyncTearDown(self):
        await self._drop_tables()
        await self._session_pool._pool.close()

    async def _truncate_tables(self):
        """Truncate tables before each test."""
        async with self._session_pool.session() as session:
            async with session.connection.cursor() as cursor:
                await cursor.execute("TRUNCATE TABLE %s" % self._outbox_table)
                await cursor.execute("TRUNCATE TABLE %s" % self._offsets_table)

    async def _drop_tables(self):
        """Drop tables after tests."""
        async with self._session_pool.session() as session:
            async with session.connection.cursor() as cursor:
                await cursor.execute("DROP TABLE IF EXISTS %s" % self._outbox_table)
                await cursor.execute("DROP TABLE IF EXISTS %s" % self._offsets_table)

    async def _publisher(self, message: OutboxMessage):
        """Test publisher that collects messages."""
        self.published_messages.append(message)

    async def test_publish_and_dispatch(self):
        """publish() stores message, dispatch() publishes it."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                message = OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "123", "amount": 100},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440001"},
                )
                await self.outbox.publish(tx_session, message)

        result = await self.outbox.dispatch(self._publisher)

        self.assertTrue(result)
        self.assertEqual(len(self.published_messages), 1)
        self.assertEqual(self.published_messages[0].uri, "kafka://orders")
        self.assertEqual(self.published_messages[0].payload["order_id"], "123")

    async def test_dispatch_returns_false_when_empty(self):
        """dispatch() returns False when no messages."""
        result = await self.outbox.dispatch(self._publisher)

        self.assertFalse(result)
        self.assertEqual(len(self.published_messages), 0)

    async def test_dispatch_updates_position(self):
        """dispatch() updates consumer position after publishing."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                message = OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "123"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440002"},
                )
                await self.outbox.publish(tx_session, message)

        await self.outbox.dispatch(self._publisher, consumer_group="test-group")

        # Dispatch again - should return False (no new messages)
        result = await self.outbox.dispatch(self._publisher, consumer_group="test-group")
        self.assertFalse(result)

    async def test_multiple_consumer_groups(self):
        """Different consumer groups track positions independently."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                message = OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "123"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440003"},
                )
                await self.outbox.publish(tx_session, message)

        # First consumer group
        result1 = await self.outbox.dispatch(self._publisher, consumer_group="group-1")
        self.assertTrue(result1)
        self.assertEqual(len(self.published_messages), 1)

        # Second consumer group - same message
        result2 = await self.outbox.dispatch(self._publisher, consumer_group="group-2")
        self.assertTrue(result2)
        self.assertEqual(len(self.published_messages), 2)

        # Both received the same message
        self.assertEqual(self.published_messages[0].uri, "kafka://orders")
        self.assertEqual(self.published_messages[1].uri, "kafka://orders")

    async def test_ordering_by_position(self):
        """Messages are dispatched in order of position."""
        async with self._session_pool.session() as session:
            for i in range(3):
                async with session.atomic() as tx_session:
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544000%d" % i},
                    )
                    await self.outbox.publish(tx_session, message)

        # Dispatch all
        while await self.outbox.dispatch(self._publisher):
            pass

        self.assertEqual(len(self.published_messages), 3)
        for i, msg in enumerate(self.published_messages):
            self.assertEqual(msg.payload["order"], i)

    async def test_batch_dispatch(self):
        """dispatch() processes batch of messages."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                for i in range(5):
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544010%d" % i},
                    )
                    await self.outbox.publish(tx_session, message)

        # Single dispatch should get all messages in batch
        result = await self.outbox.dispatch(self._publisher)

        self.assertTrue(result)
        self.assertEqual(len(self.published_messages), 5)

    async def test_get_and_set_position(self):
        """get_position() and set_position() work correctly."""
        async with self._session_pool.session() as session:
            # Initially (0, 0)
            pos = await self.outbox.get_position(session, "test-group")
            self.assertEqual(pos, (0, 0))

            # Set position (without uri filter - tracks all URIs)
            await self.outbox.set_position(session, "test-group", uri="", transaction_id=100, offset=50)

            # Get updated position
            pos = await self.outbox.get_position(session, "test-group")
            self.assertEqual(pos, (100, 50))

    async def test_get_and_set_position_with_uri(self):
        """get_position() and set_position() work with uri filter."""
        async with self._session_pool.session() as session:
            # Set position for specific uri
            await self.outbox.set_position(session, "test-group", uri="kafka://orders", transaction_id=100, offset=50)
            await self.outbox.set_position(session, "test-group", uri="kafka://users", transaction_id=200, offset=30)

            # Get positions - should be independent
            pos_orders = await self.outbox.get_position(session, "test-group", uri="kafka://orders")
            pos_users = await self.outbox.get_position(session, "test-group", uri="kafka://users")

            self.assertEqual(pos_orders, (100, 50))
            self.assertEqual(pos_users, (200, 30))

    async def test_async_iterator(self):
        """Async iterator yields messages."""
        async with self._session_pool.session() as session:
            for i in range(2):
                async with session.atomic() as tx_session:
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544020%d" % i},
                    )
                    await self.outbox.publish(tx_session, message)

        messages = []
        iterator = self.outbox.__aiter__()

        # Get first message
        message = await iterator.__anext__()
        messages.append(message)

        # Get second message
        message = await iterator.__anext__()
        messages.append(message)

        await iterator.aclose()

        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].payload["order"], 0)
        self.assertEqual(messages[1].payload["order"], 1)

    async def test_run_with_single_worker(self):
        """run() with single worker processes messages."""
        async with self._session_pool.session() as session:
            for i in range(3):
                async with session.atomic() as tx_session:
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544030%d" % i},
                    )
                    await self.outbox.publish(tx_session, message)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.2)
            stop_event.set()

        await asyncio.gather(
            self.outbox.run(self._publisher, concurrency=1, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        self.assertEqual(len(self.published_messages), 3)

    async def test_run_with_multiple_workers(self):
        """run() with multiple workers processes messages."""
        async with self._session_pool.session() as session:
            for i in range(10):
                async with session.atomic() as tx_session:
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544040%d" % i},
                    )
                    await self.outbox.publish(tx_session, message)

        # Run with multiple workers and graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            self.outbox.run(self._publisher, concurrency=3, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        # All messages should be processed
        self.assertEqual(len(self.published_messages), 10)

    async def test_visibility_rule(self):
        """Messages from uncommitted transactions are not visible."""
        # Start transaction but don't commit
        async with self._session_pool._pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO %s (uri, payload, metadata, transaction_id)
                        VALUES ('kafka://orders', '{"type": "OrderCreated", "order": 1}'::jsonb,
                                '{"event_id": "550e8400-e29b-41d4-a716-446655440050"}'::jsonb,
                                pg_current_xact_id())
                    """ % self._outbox_table)

                    # Before commit - message should not be visible to dispatcher
                    result = await self.outbox.dispatch(self._publisher)
                    self.assertFalse(result)

                # Transaction commits here

        # After commit - message should be visible
        result = await self.outbox.dispatch(self._publisher)
        self.assertTrue(result)
        self.assertEqual(len(self.published_messages), 1)

    async def test_dispatch_with_uri_filter(self):
        """dispatch() with uri filter only processes messages with that uri."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                # Publish to different URIs
                await self.outbox.publish(tx_session, OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "1"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440080"},
                ))
                await self.outbox.publish(tx_session, OutboxMessage(
                    uri="kafka://users",
                    payload={"type": "UserCreated", "user_id": "1"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440081"},
                ))
                await self.outbox.publish(tx_session, OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderShipped", "order_id": "1"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440082"},
                ))

        # Dispatch only kafka://orders
        result = await self.outbox.dispatch(self._publisher, consumer_group="orders-consumer", uri="kafka://orders")
        self.assertTrue(result)
        self.assertEqual(len(self.published_messages), 2)
        self.assertTrue(all(m.uri == "kafka://orders" for m in self.published_messages))

        # Dispatch kafka://orders again - should return False (already processed)
        result = await self.outbox.dispatch(self._publisher, consumer_group="orders-consumer", uri="kafka://orders")
        self.assertFalse(result)

        # Dispatch kafka://users - should work (separate position tracking)
        result = await self.outbox.dispatch(self._publisher, consumer_group="orders-consumer", uri="kafka://users")
        self.assertTrue(result)
        self.assertEqual(len(self.published_messages), 3)
        self.assertEqual(self.published_messages[2].uri, "kafka://users")

    async def test_multiple_uris_independent_positions(self):
        """Different URIs track positions independently for same consumer group."""
        async with self._session_pool.session() as session:
            for i in range(3):
                async with session.atomic() as tx_session:
                    await self.outbox.publish(tx_session, OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544009%d" % i},
                    ))
                    await self.outbox.publish(tx_session, OutboxMessage(
                        uri="kafka://users",
                        payload={"type": "UserCreated", "user": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-44665544019%d" % i},
                    ))

        # Consumer 1 processes orders
        orders_messages = []
        async def orders_publisher(msg):
            orders_messages.append(msg)

        while await self.outbox.dispatch(orders_publisher, consumer_group="group1", uri="kafka://orders"):
            pass

        # Consumer 2 processes users
        users_messages = []
        async def users_publisher(msg):
            users_messages.append(msg)

        while await self.outbox.dispatch(users_publisher, consumer_group="group1", uri="kafka://users"):
            pass

        # Both should get all their messages
        self.assertEqual(len(orders_messages), 3)
        self.assertEqual(len(users_messages), 3)
        self.assertTrue(all(m.uri == "kafka://orders" for m in orders_messages))
        self.assertTrue(all(m.uri == "kafka://users" for m in users_messages))

    async def test_idempotency_via_event_id(self):
        """Duplicate event_id causes unique constraint violation."""
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                message = OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "123"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440060"},
                )
                await self.outbox.publish(tx_session, message)

        # Try to insert duplicate
        with self.assertRaises(Exception):  # UniqueViolation
            async with self._session_pool.session() as session:
                async with session.atomic() as tx_session:
                    message = OutboxMessage(
                        uri="kafka://orders",
                        payload={"type": "OrderCreated", "order_id": "456"},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-446655440060"},
                    )
                    await self.outbox.publish(tx_session, message)

    async def test_workers_share_uris_without_gaps_or_overlap(self):
        """Every keyed URI is dispatched by exactly one of several workers."""
        total = 40
        num_workers = 3

        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                for i in range(total):
                    await self.outbox.publish(tx_session, OutboxMessage(
                        uri="kafka://orders/order-%d" % i,
                        payload={"type": "OrderCreated", "order": i},
                        metadata={"event_id": "550e8400-e29b-41d4-a716-4466554407%02d" % i},
                    ))

        # The set must contain URIs with a negative hashtext(), otherwise the
        # test would pass with a partition filter that ignores the sign.
        async with self._session_pool.session() as session:
            async with session.atomic():
                async with session.connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT count(*) FROM %s WHERE hashtext(uri) < 0" % self._outbox_table
                    )
                    row = await cursor.fetchone()
        self.assertGreater(row[0], 0)

        for worker_id in range(num_workers):
            while await self.outbox.dispatch(
                self._publisher, "group", worker_id=worker_id, num_workers=num_workers
            ):
                pass

        delivered_by_uri = {}
        for message in self.published_messages:
            delivered_by_uri[message.uri] = delivered_by_uri.get(message.uri, 0) + 1

        self.assertEqual(len(delivered_by_uri), total)
        self.assertEqual(set(delivered_by_uri.values()), {1})


class OutboxConcurrencyTestCase(IsolatedAsyncioTestCase):
    """Tests for concurrent dispatcher behavior."""

    _outbox_table: str = 'outbox_concurrency_test'
    _offsets_table: str = 'outbox_offsets_concurrency_test'

    async def asyncSetUp(self):
        self._session_pool = await make_pg_session_pool()
        self.outbox = Outbox(
            self._session_pool,
            outbox_table=self._outbox_table,
            offsets_table=self._offsets_table,
        )
        async with self._session_pool.session() as session:
            async with session.atomic():
                await self.outbox.setup(session)
        await self._truncate_tables()
        self.published_messages = []

    async def asyncTearDown(self):
        await self._drop_tables()
        await self._session_pool._pool.close()

    async def _truncate_tables(self):
        """Truncate tables before each test."""
        async with self._session_pool.session() as session:
            async with session.connection.cursor() as cursor:
                await cursor.execute("TRUNCATE TABLE %s" % self._outbox_table)
                await cursor.execute("TRUNCATE TABLE %s" % self._offsets_table)

    async def _drop_tables(self):
        """Drop tables after tests."""
        async with self._session_pool.session() as session:
            async with session.connection.cursor() as cursor:
                await cursor.execute("DROP TABLE IF EXISTS %s" % self._outbox_table)
                await cursor.execute("DROP TABLE IF EXISTS %s" % self._offsets_table)

    async def _publisher(self, message: OutboxMessage):
        """Test publisher that collects messages with small delay."""
        await asyncio.sleep(0.01)  # Simulate network delay
        self.published_messages.append(message)

    async def test_for_update_prevents_duplicate_processing(self):
        """FOR UPDATE lock prevents duplicate message processing."""
        # Publish single message
        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                message = OutboxMessage(
                    uri="kafka://orders",
                    payload={"type": "OrderCreated", "order_id": "123"},
                    metadata={"event_id": "550e8400-e29b-41d4-a716-446655440070"},
                )
                await self.outbox.publish(tx_session, message)

        # Run multiple concurrent dispatchers
        results = await asyncio.gather(
            self.outbox.dispatch(self._publisher, "test-group"),
            self.outbox.dispatch(self._publisher, "test-group"),
            self.outbox.dispatch(self._publisher, "test-group"),
        )

        # Only one should succeed
        self.assertEqual(sum(results), 1)
        self.assertEqual(len(self.published_messages), 1)


if __name__ == '__main__':
    unittest.main()
