"""Tests for Inbox class."""

import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.inbox.inbox import Inbox
from ascetic_ddd.inbox.message import InboxMessage
from ascetic_ddd.utils.tests.payload import json_payload


class MockCursor:
    """Mock database cursor."""

    def __init__(self, rows: list = None):
        self._rows = rows or []
        self._index = 0
        self.executed_sql = []
        self.executed_params = []

    async def execute(self, sql: str, params: tuple = None):
        self.executed_sql.append(sql)
        self.executed_params.append(params)

    async def fetchone(self):
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        return None

    async def fetchall(self):
        return self._rows

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


class InboxReceiveTestCase(IsolatedAsyncioTestCase):
    """Test cases for Inbox.publish()."""

    async def test_receive_inserts_message(self):
        """publish() inserts message into database."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated", "amount": 100}),
            metadata={"message_id": "uuid-123"},
        )

        await inbox.publish(message)

        self.assertEqual(len(cursor.executed_sql), 1)
        self.assertIn("INSERT INTO", cursor.executed_sql[0])
        self.assertIn("ON CONFLICT", cursor.executed_sql[0])

        params = cursor.executed_params[0]
        self.assertEqual(params[0], "tenant1")
        self.assertEqual(params[1], "Order")
        self.assertEqual(params[2].obj, {"id": "order-123"})
        self.assertEqual(params[3], 1)
        self.assertEqual(params[4], "kafka://orders")


class InboxDispatchTestCase(IsolatedAsyncioTestCase):
    """Test cases for Inbox.dispatch()."""

    async def test_dispatch_returns_false_when_no_messages(self):
        """dispatch() returns False when no unprocessed messages."""
        cursor = MockCursor(rows=[])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        handled = []

        async def subscriber(sess, msg):
            handled.append(msg)

        result = await inbox.dispatch(subscriber)

        self.assertFalse(result)
        self.assertEqual(len(handled), 0)

    async def test_dispatch_processes_message_without_dependencies(self):
        """dispatch() processes message without causal dependencies."""
        row = (
            "tenant1",  # tenant_id
            "Order",  # stream_type
            {"id": "order-123"},  # stream_id
            1,  # stream_position
            "kafka://orders",  # uri
            json_payload({"type": "OrderCreated", "amount": 100}),  # payload
            None,  # metadata
            1,  # received_position
            None,  # processed_position
        )
        cursor = MockCursor(rows=[row])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        handled = []

        async def subscriber(sess, msg):
            handled.append(msg)

        result = await inbox.dispatch(subscriber)

        self.assertTrue(result)
        self.assertEqual(len(handled), 1)
        self.assertEqual(handled[0].tenant_id, "tenant1")


class InboxDependencyCheckTestCase(IsolatedAsyncioTestCase):
    """Test cases for causal dependency checking."""

    async def test_dependencies_satisfied_when_empty(self):
        """Message with no dependencies is processable."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"type": "OrderCreated"}),
            metadata=None,
        )

        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        result = await inbox._are_dependencies_satisfied(session, message)

        self.assertTrue(result)

    async def test_dependency_satisfied_when_processed(self):
        """Dependency is satisfied when it exists and is processed."""
        cursor = MockCursor(rows=[(1,)])  # Row exists
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        dep = {
            "tenant_id": "tenant1",
            "stream_type": "User",
            "stream_id": {"id": "user-1"},
            "stream_position": 5,
        }

        result = await inbox._is_dependency_processed(session, dep)

        self.assertTrue(result)

    async def test_dependency_not_satisfied_when_missing(self):
        """Dependency is not satisfied when it doesn't exist."""
        cursor = MockCursor(rows=[])  # No row
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        dep = {
            "tenant_id": "tenant1",
            "stream_type": "User",
            "stream_id": {"id": "user-1"},
            "stream_position": 5,
        }

        result = await inbox._is_dependency_processed(session, dep)

        self.assertFalse(result)


class InboxSetupTestCase(IsolatedAsyncioTestCase):
    """Test cases for Inbox.setup()."""

    async def test_setup_creates_sequence_and_table(self):
        """setup() creates sequence and table."""
        cursor = MockCursor()
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        await inbox.setup(session)

        self.assertEqual(len(cursor.executed_sql), 2)
        self.assertIn("CREATE SEQUENCE", cursor.executed_sql[0])
        self.assertIn("CREATE TABLE", cursor.executed_sql[1])


class InboxAsyncIteratorTestCase(IsolatedAsyncioTestCase):
    """Test cases for Inbox async iterator."""

    async def test_aiter_yields_session_and_message(self):
        """Async iterator yields (session, message) tuples."""
        row = (
            "tenant1",
            "Order",
            {"id": "order-123"},
            1,
            "kafka://orders",
            json_payload({"type": "OrderCreated", "amount": 100}),
            None,
            1,
            None,
        )
        # First call returns row, second returns None (to stop iteration)
        cursor = MockCursor(rows=[row, None])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)

        # Use anext to get one item without infinite loop
        iterator = inbox.__aiter__()
        session_result, message_result = await iterator.__anext__()

        self.assertIsNotNone(session_result)
        self.assertEqual(message_result.tenant_id, "tenant1")
        self.assertEqual(message_result.uri, "kafka://orders")

    async def test_aiter_marks_message_as_processed(self):
        """Async iterator marks message as processed after yield."""
        row = (
            "tenant1",
            "Order",
            {"id": "order-123"},
            1,
            "kafka://orders",
            json_payload({"type": "OrderCreated", "amount": 100}),
            None,
            1,
            None,
        )
        cursor = MockCursor(rows=[row])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)

        iterator = inbox.__aiter__()
        await iterator.__anext__()
        # Close the generator to ensure code after yield executes
        await iterator.aclose()

        # Check that UPDATE was executed to mark as processed
        # Filter for UPDATE statements (not SELECT ... FOR UPDATE)
        update_sqls = [sql for sql in cursor.executed_sql if sql.strip().startswith("UPDATE")]
        self.assertEqual(len(update_sqls), 1)
        self.assertIn("processed_position", update_sqls[0])


class InboxRunTestCase(IsolatedAsyncioTestCase):
    """Test cases for Inbox.run()."""

    async def test_run_single_worker_processes_messages(self):
        """run() with single worker processes messages."""
        row = (
            "tenant1",
            "Order",
            {"id": "order-123"},
            1,
            "kafka://orders",
            json_payload({"type": "OrderCreated", "amount": 100}),
            None,
            1,
            None,
        )
        cursor = MockCursor(rows=[row])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        handled = []

        async def subscriber(sess, msg):
            handled.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            inbox.run(subscriber, concurrency=1, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        # Message should be processed
        self.assertEqual(len(handled), 1)

    async def test_run_multiple_workers_spawns_tasks(self):
        """run() with multiple workers spawns concurrent tasks."""
        rows = [
            (
                "tenant1",
                "Order",
                {"id": "order-%d" % i},
                i,
                "kafka://orders",
                json_payload({"type": "OrderCreated", "amount": 100}),
                None,
                i,
                None,
            )
            for i in range(4)
        ]
        cursor = MockCursor(rows=rows)
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        handled = []

        async def subscriber(sess, msg):
            handled.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            inbox.run(subscriber, concurrency=2, poll_interval=0.01, stop_event=stop_event),
            stop_after_delay(),
        )

        # Multiple messages should be processed
        self.assertGreaterEqual(len(handled), 1)

    async def test_run_worker_sleeps_when_no_messages(self):
        """run() worker sleeps when no messages available."""
        cursor = MockCursor(rows=[])
        connection = MockConnection(cursor)
        session = MockSession(connection)
        pool = MockSessionPool(session)

        inbox = Inbox(pool)
        handled = []

        async def subscriber(sess, msg):
            handled.append(msg)

        # Run with graceful shutdown
        stop_event = asyncio.Event()

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            inbox.run(subscriber, concurrency=1, poll_interval=0.05, stop_event=stop_event),
            stop_after_delay(),
        )

        # No messages processed
        self.assertEqual(len(handled), 0)


if __name__ == '__main__':
    unittest.main()
