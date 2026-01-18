"""Tests for query collector classes."""
import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock

from ascetic_ddd.deferred.deferred import Deferred

from ...batch.query_collector import (
    QueryCollector,
    ConnectionCollector,
    CursorCollector,
)
from ...batch.multi_query import MultiQuery, AutoincrementMultiInsertQuery


class CursorCollectorTestCase(TestCase):
    """Tests for CursorCollector class."""

    def test_execute_calls_collect_query(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        cursor = CursorCollector(collect_fn)

        asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))

        collect_fn.assert_called_once_with("INSERT INTO t (a) VALUES (%s)", (1,))

    def test_execute_returns_self(self):
        collect_fn = MagicMock(return_value=Deferred())
        cursor = CursorCollector(collect_fn)

        result = asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))

        self.assertIs(result, cursor)

    def test_fetchone_returns_deferred(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        cursor = CursorCollector(collect_fn)

        asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))
        result = asyncio.run(cursor.fetchone())

        self.assertIs(result, deferred)

    def test_fetchone_returns_none_before_execute(self):
        collect_fn = MagicMock(return_value=Deferred())
        cursor = CursorCollector(collect_fn)

        result = asyncio.run(cursor.fetchone())

        self.assertIsNone(result)

    def test_fetchmany_returns_deferred_list(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        cursor = CursorCollector(collect_fn)

        asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))
        result = asyncio.run(cursor.fetchmany())

        self.assertIsInstance(result, Deferred)

        # Resolve source deferred with a row
        deferred.resolve({"id": 1})

        # Result deferred should now be resolved with list
        self.assertTrue(result._is_resolved)
        self.assertEqual(result._value, [{"id": 1}])

    def test_fetchmany_with_none_row(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        cursor = CursorCollector(collect_fn)

        asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))
        result = asyncio.run(cursor.fetchmany())

        # Resolve source deferred with None
        deferred.resolve(None)

        # Result should be empty list
        self.assertTrue(result._is_resolved)
        self.assertEqual(result._value, [])

    def test_fetchall_same_as_fetchmany(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        cursor = CursorCollector(collect_fn)

        asyncio.run(cursor.execute("INSERT INTO t (a) VALUES (%s)", (1,)))
        result = asyncio.run(cursor.fetchall())

        deferred.resolve({"id": 1})

        self.assertTrue(result._is_resolved)
        self.assertEqual(result._value, [{"id": 1}])

    def test_close_is_noop(self):
        cursor = CursorCollector(MagicMock())
        # Should not raise
        asyncio.run(cursor.close())

    def test_async_context_manager(self):
        cursor = CursorCollector(MagicMock())

        async def run():
            async with cursor as c:
                self.assertIs(c, cursor)

        asyncio.run(run())


class ConnectionCollectorTestCase(TestCase):
    """Tests for ConnectionCollector class."""

    def test_cursor_returns_cursor_collector(self):
        collect_fn = MagicMock()
        conn = ConnectionCollector(collect_fn)

        cursor = conn.cursor()

        self.assertIsInstance(cursor, CursorCollector)

    def test_execute_returns_cursor_after_execute(self):
        deferred = Deferred()
        collect_fn = MagicMock(return_value=deferred)
        conn = ConnectionCollector(collect_fn)

        cursor = asyncio.run(conn.execute("INSERT INTO t (a) VALUES (%s)", (1,)))

        self.assertIsInstance(cursor, CursorCollector)
        collect_fn.assert_called_once()

    def test_transaction_raises_not_implemented(self):
        conn = ConnectionCollector(MagicMock())

        with self.assertRaises(NotImplementedError):
            conn.transaction()

    def test_close_is_noop(self):
        conn = ConnectionCollector(MagicMock())
        # Should not raise
        asyncio.run(conn.close())

    def test_async_context_manager(self):
        conn = ConnectionCollector(MagicMock())

        async def run():
            async with conn as c:
                self.assertIs(c, conn)

        asyncio.run(run())


class QueryCollectorTestCase(TestCase):
    """Tests for QueryCollector class."""

    def test_has_connection_property(self):
        collector = QueryCollector()

        self.assertIsInstance(collector.connection, ConnectionCollector)

    def test_len_returns_number_of_query_types(self):
        collector = QueryCollector()

        self.assertEqual(len(collector), 0)

        asyncio.run(collector.connection.execute(
            "INSERT INTO t1 (a) VALUES (%s)", (1,)
        ))
        self.assertEqual(len(collector), 1)

        asyncio.run(collector.connection.execute(
            "INSERT INTO t2 (a) VALUES (%s)", (2,)
        ))
        self.assertEqual(len(collector), 2)

        # Same query template should not increase count
        asyncio.run(collector.connection.execute(
            "INSERT INTO t1 (a) VALUES (%s)", (3,)
        ))
        self.assertEqual(len(collector), 2)

    def test_clear_removes_all_queries(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s)", (1,)
        ))
        self.assertEqual(len(collector), 1)

        collector.clear()
        self.assertEqual(len(collector), 0)

    def test_collect_insert_creates_multi_query(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s)", (1,)
        ))

        self.assertEqual(len(collector._multi_query_map), 1)
        query = list(collector._multi_query_map.values())[0]
        self.assertIsInstance(query, MultiQuery)

    def test_collect_insert_with_returning_creates_autoincrement_query(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s) RETURNING id", (1,)
        ))

        self.assertEqual(len(collector._multi_query_map), 1)
        query = list(collector._multi_query_map.values())[0]
        self.assertIsInstance(query, AutoincrementMultiInsertQuery)

    def test_collect_non_insert_returns_resolved_deferred(self):
        collector = QueryCollector()

        cursor = asyncio.run(collector.connection.execute(
            "SELECT * FROM t WHERE id = %s", (1,)
        ))

        self.assertEqual(len(collector._multi_query_map), 0)

        deferred = asyncio.run(cursor.fetchone())
        self.assertTrue(deferred._is_resolved)
        self.assertIsNone(deferred._value)

    def test_evaluate_empty(self):
        collector = QueryCollector()
        session = MagicMock()

        asyncio.run(collector.evaluate(session))

        session.connection.execute.assert_not_called()

    def test_evaluate_executes_batched_inserts(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a, b) VALUES (%s, %s)", (1, "x")
        ))
        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a, b) VALUES (%s, %s)", (2, "y")
        ))

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(collector.evaluate(session))

        session.connection.execute.assert_called_once_with(
            "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s)",
            (1, "x", 2, "y")
        )

    def test_evaluate_executes_multiple_query_types(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t1 (a) VALUES (%s)", (1,)
        ))
        asyncio.run(collector.connection.execute(
            "INSERT INTO t2 (a) VALUES (%s)", (2,)
        ))

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(collector.evaluate(session))

        self.assertEqual(session.connection.execute.call_count, 2)

    def test_evaluate_clears_map_after_execution(self):
        collector = QueryCollector()

        asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s)", (1,)
        ))
        self.assertEqual(len(collector), 1)

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(collector.evaluate(session))

        self.assertEqual(len(collector), 0)

    def test_evaluate_resolves_deferreds(self):
        collector = QueryCollector()

        cursor1 = asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s)", (1,)
        ))
        cursor2 = asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s)", (2,)
        ))

        d1 = asyncio.run(cursor1.fetchone())
        d2 = asyncio.run(cursor2.fetchone())

        self.assertFalse(d1._is_resolved)
        self.assertFalse(d2._is_resolved)

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(collector.evaluate(session))

        self.assertTrue(d1._is_resolved)
        self.assertTrue(d2._is_resolved)

    def test_evaluate_with_returning_resolves_with_rows(self):
        collector = QueryCollector()

        cursor1 = asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s) RETURNING id", ("x",)
        ))
        cursor2 = asyncio.run(collector.connection.execute(
            "INSERT INTO t (a) VALUES (%s) RETURNING id", ("y",)
        ))

        d1 = asyncio.run(cursor1.fetchone())
        d2 = asyncio.run(cursor2.fetchone())

        db_cursor = AsyncMock()
        db_cursor.fetchall = AsyncMock(return_value=[(100,), (101,)])

        session = MagicMock()
        session.connection.execute = AsyncMock(return_value=db_cursor)

        asyncio.run(collector.evaluate(session))

        self.assertEqual(d1._value, (100,))
        self.assertEqual(d2._value, (101,))


class QueryCollectorNestedQueriesTestCase(TestCase):
    """Tests for QueryCollector handling nested queries via deferred callbacks."""

    def test_evaluate_handles_nested_inserts(self):
        """Test that queries added during evaluation are also processed."""
        collector = QueryCollector()

        # First insert
        cursor1 = asyncio.run(collector.connection.execute(
            "INSERT INTO parent (name) VALUES (%s) RETURNING id", ("parent1",)
        ))
        d1 = asyncio.run(cursor1.fetchone())

        # Register callback that adds another insert when parent is resolved
        # Note: callback runs synchronously during resolve(), so we use
        # _collect_query directly instead of async connection.execute()
        nested_deferred = None

        def on_parent_resolved(row):
            nonlocal nested_deferred
            parent_id = row[0]
            # Directly call _collect_query (sync) to add nested query
            nested_deferred = collector._collect_query(
                "INSERT INTO child (parent_id) VALUES (%s)", (parent_id,)
            )
            return None

        d1.then(on_parent_resolved, lambda e: None)

        # Setup mock session
        parent_cursor = AsyncMock()
        parent_cursor.fetchall = AsyncMock(return_value=[(42,)])

        child_cursor = AsyncMock()

        call_count = [0]

        async def mock_execute(sql, params):
            call_count[0] += 1
            if "parent" in sql:
                return parent_cursor
            return child_cursor

        session = MagicMock()
        session.connection.execute = mock_execute

        # Evaluate should process both parent and child inserts
        asyncio.run(collector.evaluate(session))

        # Parent should be resolved with row
        self.assertEqual(d1._value, (42,))

        # Child should have been added and resolved
        self.assertIsNotNone(nested_deferred)
        self.assertTrue(nested_deferred._is_resolved)

        # Both queries should have been executed
        self.assertEqual(call_count[0], 2)
