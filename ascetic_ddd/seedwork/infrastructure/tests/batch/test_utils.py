"""Tests for batch utility functions."""
from unittest import TestCase

from ...batch.utils import (
    is_insert_query,
    is_autoincrement_insert_query,
    convert_named_to_positional,
    RE_INSERT_VALUES,
    RE_NAMED_PARAM,
)


class IsInsertQueryTestCase(TestCase):
    """Tests for is_insert_query function."""

    def test_simple_insert(self):
        query = "INSERT INTO users (name) VALUES (%s)"
        self.assertTrue(is_insert_query(query))

    def test_insert_multiple_columns(self):
        query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)"
        self.assertTrue(is_insert_query(query))

    def test_insert_with_returning_is_false(self):
        query = "INSERT INTO users (name) VALUES (%s) RETURNING id"
        self.assertFalse(is_insert_query(query))

    def test_insert_lowercase(self):
        query = "insert into users (name) values (%s)"
        self.assertTrue(is_insert_query(query))

    def test_insert_with_whitespace(self):
        query = "  INSERT INTO users (name) VALUES (%s)  "
        self.assertTrue(is_insert_query(query))

    def test_select_is_false(self):
        query = "SELECT * FROM users"
        self.assertFalse(is_insert_query(query))

    def test_update_is_false(self):
        query = "UPDATE users SET name = %s WHERE id = %s"
        self.assertFalse(is_insert_query(query))

    def test_delete_is_false(self):
        query = "DELETE FROM users WHERE id = %s"
        self.assertFalse(is_insert_query(query))


class IsAutoincrementInsertQueryTestCase(TestCase):
    """Tests for is_autoincrement_insert_query function."""

    def test_insert_with_returning(self):
        query = "INSERT INTO users (name) VALUES (%s) RETURNING id"
        self.assertTrue(is_autoincrement_insert_query(query))

    def test_insert_with_returning_multiple_columns(self):
        query = "INSERT INTO users (name) VALUES (%s) RETURNING id, created_at"
        self.assertTrue(is_autoincrement_insert_query(query))

    def test_insert_without_returning_is_false(self):
        query = "INSERT INTO users (name) VALUES (%s)"
        self.assertFalse(is_autoincrement_insert_query(query))

    def test_returning_lowercase(self):
        query = "insert into users (name) values (%s) returning id"
        self.assertTrue(is_autoincrement_insert_query(query))

    def test_select_is_false(self):
        query = "SELECT * FROM users"
        self.assertFalse(is_autoincrement_insert_query(query))


class ReInsertValuesTestCase(TestCase):
    """Tests for RE_INSERT_VALUES regex pattern."""

    def test_match_simple_values(self):
        query = "INSERT INTO t (a) VALUES (%s)"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%s)")

    def test_match_multiple_placeholders(self):
        query = "INSERT INTO t (a, b, c) VALUES (%s, %s, %s)"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%s, %s, %s)")

    def test_match_with_returning(self):
        query = "INSERT INTO t (a, b) VALUES (%s, %s) RETURNING id"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%s, %s)")

    def test_match_lowercase(self):
        query = "insert into t (a) values (%s)"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%s)")

    def test_no_match_select(self):
        query = "SELECT * FROM users"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNone(match)

    def test_substitution(self):
        query = "INSERT INTO t (a, b) VALUES (%s, %s) RETURNING id"
        pattern = "(%s, %s)"
        combined = f"{pattern}, {pattern}, {pattern}"
        result = RE_INSERT_VALUES.sub(f"VALUES {combined}", query)
        self.assertEqual(
            result,
            "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s), (%s, %s) RETURNING id"
        )

    def test_match_named_params(self):
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%(a)s, %(b)s)")

    def test_match_named_params_with_returning(self):
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s) RETURNING id"
        match = RE_INSERT_VALUES.search(query)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "(%(a)s, %(b)s)")


class ReNamedParamTestCase(TestCase):
    """Tests for RE_NAMED_PARAM regex pattern."""

    def test_findall_single_param(self):
        query = "INSERT INTO t (a) VALUES (%(a)s)"
        names = RE_NAMED_PARAM.findall(query)
        self.assertEqual(names, ["a"])

    def test_findall_multiple_params(self):
        query = "INSERT INTO t (a, b, c) VALUES (%(a)s, %(b)s, %(c)s)"
        names = RE_NAMED_PARAM.findall(query)
        self.assertEqual(names, ["a", "b", "c"])

    def test_findall_preserves_order(self):
        query = "INSERT INTO t (x, y, z) VALUES (%(z)s, %(x)s, %(y)s)"
        names = RE_NAMED_PARAM.findall(query)
        self.assertEqual(names, ["z", "x", "y"])

    def test_sub_converts_to_positional(self):
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        result = RE_NAMED_PARAM.sub("%s", query)
        self.assertEqual(result, "INSERT INTO t (a, b) VALUES (%s, %s)")


class ConvertNamedToPositionalTestCase(TestCase):
    """Tests for convert_named_to_positional function."""

    def test_simple_conversion(self):
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        params = {"a": 1, "b": "x"}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(converted_query, "INSERT INTO t (a, b) VALUES (%s, %s)")
        self.assertEqual(positional_params, (1, "x"))

    def test_preserves_param_order(self):
        query = "INSERT INTO t (x, y, z) VALUES (%(z)s, %(x)s, %(y)s)"
        params = {"x": 1, "y": 2, "z": 3}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(converted_query, "INSERT INTO t (x, y, z) VALUES (%s, %s, %s)")
        # Order should match appearance in query: z, x, y
        self.assertEqual(positional_params, (3, 1, 2))

    def test_with_returning(self):
        query = "INSERT INTO t (a) VALUES (%(a)s) RETURNING id"
        params = {"a": 42}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(converted_query, "INSERT INTO t (a) VALUES (%s) RETURNING id")
        self.assertEqual(positional_params, (42,))

    def test_multiple_columns(self):
        query = "INSERT INTO users (name, email, age) VALUES (%(name)s, %(email)s, %(age)s)"
        params = {"name": "John", "email": "john@example.com", "age": 30}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(
            converted_query,
            "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)"
        )
        self.assertEqual(positional_params, ("John", "john@example.com", 30))

    def test_with_none_value(self):
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        params = {"a": None, "b": "x"}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(positional_params, (None, "x"))

    def test_repeated_param(self):
        """Test that repeated named params are expanded correctly."""
        query = "INSERT INTO t (a, b, c) VALUES (%(x)s, %(x)s, %(y)s)"
        params = {"x": 1, "y": 2}

        converted_query, positional_params = convert_named_to_positional(query, params)

        self.assertEqual(converted_query, "INSERT INTO t (a, b, c) VALUES (%s, %s, %s)")
        self.assertEqual(positional_params, (1, 1, 2))
