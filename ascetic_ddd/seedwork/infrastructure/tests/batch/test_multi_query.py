"""Tests for multi-query batch implementations."""
import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock

from ...batch.multi_query import (
    MultiQueryBase,
    MultiQuery,
    AutoincrementMultiInsertQuery,
)


class MultiQueryBaseTestCase(TestCase):
    """Tests for MultiQueryBase class."""

    def test_execute_stores_template_on_first_call(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%s, %s)"
        mq.execute(query, (1, "x"))

        self.assertEqual(mq._sql_template, query)
        self.assertEqual(mq._values_pattern, "(%s, %s)")

    def test_execute_stores_params(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a) VALUES (%s)"
        mq.execute(query, (1,))
        mq.execute(query, (2,))
        mq.execute(query, (3,))

        self.assertEqual(len(mq._params), 3)
        self.assertEqual(mq._params[0], (1,))
        self.assertEqual(mq._params[1], (2,))
        self.assertEqual(mq._params[2], (3,))

    def test_execute_returns_deferred(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a) VALUES (%s)"
        result = mq.execute(query, (1,))

        self.assertFalse(result._is_resolved)
        self.assertFalse(result._is_rejected)

    def test_execute_with_none_params(self):
        mq = MultiQuery()
        query = "INSERT INTO t DEFAULT VALUES"
        mq.execute(query, None)

        self.assertEqual(mq._params[0], ())

    def test_execute_with_bytes_query(self):
        mq = MultiQuery()
        query = b"INSERT INTO t (a) VALUES (%s)"
        mq.execute(query, (1,))

        self.assertEqual(mq._sql_template, "INSERT INTO t (a) VALUES (%s)")

    def test_execute_converts_mapping_params(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        mq.execute(query, {"a": 1, "b": "x"})

        # Should convert to positional
        self.assertEqual(mq._sql_template, "INSERT INTO t (a, b) VALUES (%s, %s)")
        self.assertEqual(mq._params[0], (1, "x"))

    def test_batch_with_mapping_params(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        mq.execute(query, {"a": 1, "b": "x"})
        mq.execute(query, {"a": 2, "b": "y"})
        mq.execute(query, {"a": 3, "b": "z"})

        sql = mq._build_sql()
        params = mq._merge_params()

        self.assertEqual(
            sql,
            "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s), (%s, %s)"
        )
        self.assertEqual(params, (1, "x", 2, "y", 3, "z"))

    def test_build_sql_single_row(self):
        mq = MultiQuery()
        mq.execute("INSERT INTO t (a, b) VALUES (%s, %s)", (1, "x"))

        sql = mq._build_sql()
        self.assertEqual(sql, "INSERT INTO t (a, b) VALUES (%s, %s)")

    def test_build_sql_multiple_rows(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%s, %s)"
        mq.execute(query, (1, "x"))
        mq.execute(query, (2, "y"))
        mq.execute(query, (3, "z"))

        sql = mq._build_sql()
        self.assertEqual(
            sql,
            "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s), (%s, %s)"
        )

    def test_build_sql_with_returning(self):
        mq = AutoincrementMultiInsertQuery()
        query = "INSERT INTO t (a) VALUES (%s) RETURNING id"
        mq.execute(query, (1,))
        mq.execute(query, (2,))

        sql = mq._build_sql()
        self.assertEqual(
            sql,
            "INSERT INTO t (a) VALUES (%s), (%s) RETURNING id"
        )

    def test_merge_params_single_row(self):
        mq = MultiQuery()
        mq.execute("INSERT INTO t (a, b) VALUES (%s, %s)", (1, "x"))

        params = mq._merge_params()
        self.assertEqual(params, (1, "x"))

    def test_merge_params_multiple_rows(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%s, %s)"
        mq.execute(query, (1, "x"))
        mq.execute(query, (2, "y"))
        mq.execute(query, (3, "z"))

        params = mq._merge_params()
        self.assertEqual(params, (1, "x", 2, "y", 3, "z"))


class MultiQueryTestCase(TestCase):
    """Tests for MultiQuery class (INSERT without RETURNING)."""

    def test_evaluate_empty(self):
        mq = MultiQuery()
        session = MagicMock()

        asyncio.run(mq.evaluate(session))

        session.connection.execute.assert_not_called()

    def test_evaluate_executes_batched_query(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a, b) VALUES (%s, %s)"
        mq.execute(query, (1, "x"))
        mq.execute(query, (2, "y"))

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(mq.evaluate(session))

        session.connection.execute.assert_called_once_with(
            "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s)",
            (1, "x", 2, "y")
        )

    def test_evaluate_resolves_deferreds_with_none(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a) VALUES (%s)"
        d1 = mq.execute(query, (1,))
        d2 = mq.execute(query, (2,))

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(mq.evaluate(session))

        self.assertTrue(d1._is_resolved)
        self.assertTrue(d2._is_resolved)
        self.assertIsNone(d1._value)
        self.assertIsNone(d2._value)

    def test_deferred_callback_called_on_resolve(self):
        mq = MultiQuery()
        query = "INSERT INTO t (a) VALUES (%s)"
        d1 = mq.execute(query, (1,))

        callback_values = []

        def on_success(value):
            callback_values.append(value)
            return None

        d1.then(on_success, lambda e: None)

        session = MagicMock()
        session.connection.execute = AsyncMock()

        asyncio.run(mq.evaluate(session))

        self.assertEqual(callback_values, [None])


class AutoincrementMultiInsertQueryTestCase(TestCase):
    """Tests for AutoincrementMultiInsertQuery class (INSERT with RETURNING)."""

    def test_evaluate_empty(self):
        mq = AutoincrementMultiInsertQuery()
        session = MagicMock()

        asyncio.run(mq.evaluate(session))

        session.connection.execute.assert_not_called()

    def test_evaluate_executes_batched_query_with_returning(self):
        mq = AutoincrementMultiInsertQuery()
        query = "INSERT INTO t (a) VALUES (%s) RETURNING id"
        mq.execute(query, (1,))
        mq.execute(query, (2,))

        cursor = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[(10,), (11,)])

        session = MagicMock()
        session.connection.execute = AsyncMock(return_value=cursor)

        asyncio.run(mq.evaluate(session))

        session.connection.execute.assert_called_once_with(
            "INSERT INTO t (a) VALUES (%s), (%s) RETURNING id",
            (1, 2)
        )

    def test_evaluate_resolves_deferreds_with_rows(self):
        mq = AutoincrementMultiInsertQuery()
        query = "INSERT INTO t (a) VALUES (%s) RETURNING id"
        d1 = mq.execute(query, ("x",))
        d2 = mq.execute(query, ("y",))
        d3 = mq.execute(query, ("z",))

        cursor = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[(100,), (101,), (102,)])

        session = MagicMock()
        session.connection.execute = AsyncMock(return_value=cursor)

        asyncio.run(mq.evaluate(session))

        self.assertTrue(d1._is_resolved)
        self.assertTrue(d2._is_resolved)
        self.assertTrue(d3._is_resolved)
        self.assertEqual(d1._value, (100,))
        self.assertEqual(d2._value, (101,))
        self.assertEqual(d3._value, (102,))

    def test_deferred_callback_receives_row(self):
        mq = AutoincrementMultiInsertQuery()
        query = "INSERT INTO t (a) VALUES (%s) RETURNING id"
        d1 = mq.execute(query, ("x",))

        received_rows = []

        def on_success(row):
            received_rows.append(row)
            return None

        d1.then(on_success, lambda e: None)

        cursor = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[(42,)])

        session = MagicMock()
        session.connection.execute = AsyncMock(return_value=cursor)

        asyncio.run(mq.evaluate(session))

        self.assertEqual(received_rows, [(42,)])
