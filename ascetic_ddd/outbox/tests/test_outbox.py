"""Unit tests for Outbox."""

import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.outbox.message import OutboxMessage
from ascetic_ddd.outbox.outbox import Outbox
from ascetic_ddd.utils.tests.payload import json_payload, decode_payload


class MockCursor:
    """Mock database cursor."""

    def __init__(self, rows: list = None, consume_on_fetch: bool = False):
        self._rows = rows or []
        self._index = 0
        self._consume_on_fetch = consume_on_fetch
        self.executed_sql = []
        self.executed_params = []

    async def execute(self, sql: str, params: dict = None):
        self.executed_sql.append(sql)
        self.executed_params.append(params)

    async def fetchone(self):
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        return None

    async def fetchall(self):
        rows = self._rows
        if self._consume_on_fetch:
            self._rows = []
        return rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockTransaction:
    """Mock database transaction."""

    def __init__(self, connection):
        self._connection = connection

    @property
    def connection(self):
        return self._connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockConnection:
    """Mock database connection."""

    def __init__(self, cursor: MockCursor = None):
        self._cursor = cursor or MockCursor()

    def cursor(self):
        return self._cursor

    def transaction(self):
        return MockTransaction(self)

    async def set_isolation_level(self, level):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockSession:
    """Mock database session."""

    def __init__(self, connection: MockConnection = None):
        self._connection = connection or MockConnection()

    @property
    def connection(self):
        return self._connection

    def atomic(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockSessionPool:
    """Mock session pool."""

    def __init__(self, session: MockSession = None):
        self._session = session or MockSession()

    def session(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *args):
        pass


class OutboxPublishTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox.publish()."""

    async def test_publish_inserts_message(self):
        """publish() inserts message into database."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        message = OutboxMessage(
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated", "order_id": "123", "amount": 100}),
            metadata={"message_id": "uuid-123", "correlation_id": "corr-456"},
        )

        await outbox.publish(session, message)

        self.assertEqual(len(cursor.executed_sql), 1)
        self.assertIn("INSERT INTO", cursor.executed_sql[0])
        self.assertIn("pg_current_xact_id()", cursor.executed_sql[0])

        params = cursor.executed_params[0]
        self.assertEqual(params["uri"], "kafka://orders")

    async def test_publish_uses_custom_table_name(self):
        """publish() uses custom table name if provided."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool, outbox_table="custom_outbox")
        message = OutboxMessage(
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated", "order_id": "123"}),
            metadata={"message_id": "uuid-123"},
        )

        await outbox.publish(session, message)

        self.assertIn("custom_outbox", cursor.executed_sql[0])


class OutboxDispatchTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox.dispatch()."""

    async def test_dispatch_returns_false_when_no_messages(self):
        """dispatch() returns False when no unprocessed messages."""
        cursor = MockCursor(rows=[])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        result = await outbox.dispatch(publisher)

        self.assertFalse(result)
        self.assertEqual(len(published), 0)

    async def test_dispatch_publishes_messages(self):
        """dispatch() calls publisher for each message."""
        rows = [
            (1, 100, "kafka://orders", json_payload({"type": "OrderCreated", "order_id": "123"}), {"message_id": "uuid-1"}, "2024-01-01 00:00:00"),
            (2, 100, "kafka://orders", json_payload({"type": "OrderShipped", "order_id": "123"}), {"message_id": "uuid-2"}, "2024-01-01 00:00:01"),
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        result = await outbox.dispatch(publisher)

        self.assertTrue(result)
        self.assertEqual(len(published), 2)
        self.assertEqual(published[0].uri, "kafka://orders")
        self.assertEqual(decode_payload(published[0].payload)["type"], "OrderCreated")
        self.assertEqual(decode_payload(published[1].payload)["type"], "OrderShipped")

    async def test_dispatch_acknowledges_last_message(self):
        """dispatch() updates consumer position after publishing."""
        rows = [
            (5, 100, "kafka://orders", json_payload({"type": "OrderCreated", "order_id": "123"}), {"message_id": "uuid-1"}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)

        async def publisher(msg):
            pass

        await outbox.dispatch(publisher, consumer_group="test-group")

        # Check that ack was called (ensure_consumer_group + actual ack)
        ack_sql = [sql for sql in cursor.executed_sql if "ON CONFLICT" in sql and "offset_acked" in sql]
        self.assertGreaterEqual(len(ack_sql), 1)

    async def test_dispatch_with_uri_filter(self):
        """dispatch() with uri filters messages by uri prefix."""
        rows = [
            (1, 100, "kafka://orders", json_payload({"type": "OrderCreated", "order_id": "123"}), {"message_id": "uuid-1"}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        result = await outbox.dispatch(publisher, consumer_group="test-group", uri="kafka://orders")

        self.assertTrue(result)
        self.assertEqual(len(published), 1)

        # Verify uri filter was applied (check params contain uri and uri_prefix)
        uri_params = [p for p in cursor.executed_params if p and p.get("uri") == "kafka://orders"]
        self.assertGreaterEqual(len(uri_params), 1)
        # Check uri_prefix for LIKE pattern
        prefix_params = [p for p in cursor.executed_params if p and p.get("uri_prefix") == "kafka://orders/%"]
        self.assertGreaterEqual(len(prefix_params), 1)

    async def test_dispatch_with_partitioning(self):
        """dispatch() with worker_id and num_workers partitions messages."""
        rows = [
            (1, 100, "kafka://orders/order-123", json_payload({"type": "OrderCreated"}), {}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        result = await outbox.dispatch(
            publisher,
            consumer_group="test-group",
            uri="kafka://orders",
            worker_id=0,
            num_workers=3
        )

        self.assertTrue(result)

        # Verify partition filter was applied
        partition_params = [p for p in cursor.executed_params if p and p.get("num_workers") == 3]
        self.assertGreaterEqual(len(partition_params), 1)
        self.assertEqual(partition_params[0]["worker_id"], 0)

        # Verify consumer_group was modified to include worker_id
        consumer_group_params = [p for p in cursor.executed_params if p and p.get("consumer_group") == "test-group:0"]
        self.assertGreaterEqual(len(consumer_group_params), 1)


class OutboxGetPositionTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox.get_position()."""

    async def test_get_position_returns_zeros_when_not_found(self):
        """get_position() returns (0, 0) when consumer group not found."""
        cursor = MockCursor(rows=[])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        result = await outbox.get_position(session, "test-group")

        self.assertEqual(result, (0, 0))

    async def test_get_position_returns_stored_position(self):
        """get_position() returns stored position."""
        cursor = MockCursor(rows=[(100, 50)])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        result = await outbox.get_position(session, "test-group")

        self.assertEqual(result, (100, 50))

    async def test_get_position_with_uri(self):
        """get_position() filters by uri."""
        cursor = MockCursor(rows=[(100, 50)])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        result = await outbox.get_position(session, "test-group", uri="kafka://orders")

        self.assertEqual(result, (100, 50))
        # Verify uri was included in query params
        params = cursor.executed_params[0]
        self.assertEqual(params["uri"], "kafka://orders")


class OutboxSetPositionTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox.set_position()."""

    async def test_set_position_upserts(self):
        """set_position() uses INSERT ... ON CONFLICT DO UPDATE."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        await outbox.set_position(session, "test-group", uri="", transaction_id=100, offset=50)

        self.assertEqual(len(cursor.executed_sql), 1)
        self.assertIn("INSERT INTO", cursor.executed_sql[0])
        self.assertIn("ON CONFLICT", cursor.executed_sql[0])
        self.assertIn("DO UPDATE", cursor.executed_sql[0])

        params = cursor.executed_params[0]
        self.assertEqual(params["consumer_group"], "test-group")
        self.assertEqual(params["uri"], "")
        self.assertEqual(params["offset"], 50)
        self.assertEqual(params["transaction_id"], "100")

    async def test_set_position_with_uri(self):
        """set_position() includes uri in upsert."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        await outbox.set_position(session, "test-group", uri="kafka://orders", transaction_id=100, offset=50)

        params = cursor.executed_params[0]
        self.assertEqual(params["consumer_group"], "test-group")
        self.assertEqual(params["uri"], "kafka://orders")
        self.assertEqual(params["offset"], 50)


class OutboxRunTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox.run()."""

    async def test_run_processes_messages(self):
        """run() processes messages."""
        rows = [
            (1, 100, "kafka://orders", json_payload({"type": "OrderCreated", "order_id": "123"}), {"message_id": "uuid-1"}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows, consume_on_fetch=True)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            outbox.run(publisher, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        self.assertGreaterEqual(len(published), 1)

    async def test_run_sleeps_when_no_messages(self):
        """run() sleeps when no messages available."""
        cursor = MockCursor(rows=[])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            outbox.run(publisher, poll_interval=0.05, stop_event=stop_event),
            stop_after_delay(),
        )

        self.assertEqual(len(published), 0)

    async def test_run_with_partitioning(self):
        """run() with partitioning distributes messages across workers."""
        rows = [
            (1, 100, "kafka://orders/order-123", json_payload({"type": "OrderCreated"}), {}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows, consume_on_fetch=True)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)
        published = []

        async def publisher(msg):
            published.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            outbox.run(
                publisher,
                uri="kafka://orders",
                process_id=0,
                num_processes=3,
                poll_interval=0.01,
                stop_event=stop_event
            ),
            stop_after_delay(),
        )

        # Verify partition filter was applied
        partition_params = [p for p in cursor.executed_params if p and p.get("num_workers") == 3]
        self.assertGreaterEqual(len(partition_params), 1)


class OutboxAsyncIteratorTestCase(IsolatedAsyncioTestCase):
    """Test cases for Outbox async iterator."""

    async def test_aiter_yields_messages(self):
        """Async iterator yields OutboxMessage."""
        rows = [
            (1, 100, "kafka://orders", json_payload({"type": "OrderCreated", "order_id": "123"}), {"message_id": "uuid-1"}, "2024-01-01 00:00:00"),
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        outbox = Outbox(pool)

        iterator = outbox.__aiter__()
        message = await iterator.__anext__()

        self.assertEqual(message.uri, "kafka://orders")
        self.assertEqual(decode_payload(message.payload)["type"], "OrderCreated")
        self.assertEqual(message.position, 1)
        self.assertEqual(message.transaction_id, 100)

        await iterator.aclose()


class OutboxMessageTestCase(unittest.TestCase):
    """Test cases for OutboxMessage."""

    def test_message_creation(self):
        """OutboxMessage is created with required fields."""
        message = OutboxMessage(
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated", "order_id": "123"}),
            metadata={"message_id": "uuid-123"},
        )

        self.assertEqual(message.uri, "kafka://orders")
        self.assertEqual(message.payload, json_payload({"type": "OrderCreated", "order_id": "123"}))
        self.assertEqual(message.metadata, {"message_id": "uuid-123"})
        self.assertIsNone(message.position)
        self.assertIsNone(message.transaction_id)

    def test_message_with_all_fields(self):
        """OutboxMessage is created with all fields."""
        message = OutboxMessage(
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated", "order_id": "123"}),
            metadata={"message_id": "uuid-123"},
            created_at="2024-01-01 00:00:00",
            position=5,
            transaction_id=100,
        )

        self.assertEqual(message.created_at, "2024-01-01 00:00:00")
        self.assertEqual(message.position, 5)
        self.assertEqual(message.transaction_id, 100)


if __name__ == '__main__':
    unittest.main()
