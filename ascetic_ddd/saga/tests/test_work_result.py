"""Tests for WorkResult class."""

import unittest

from ascetic_ddd.saga.work_result import WorkResult


class WorkResultTestCase(unittest.TestCase):
    """Test cases for WorkResult."""

    def test_create_empty(self):
        """WorkResult can be created empty."""
        result = WorkResult()
        self.assertEqual(len(result), 0)

    def test_create_with_data(self):
        """WorkResult can be created with initial data."""
        result = WorkResult({"reservationId": 12345, "status": "confirmed"})
        self.assertEqual(result["reservationId"], 12345)
        self.assertEqual(result["status"], "confirmed")

    def test_is_dict_subclass(self):
        """WorkResult is a dict subclass."""
        result = WorkResult()
        self.assertIsInstance(result, dict)

    def test_set_and_get_items(self):
        """WorkResult supports dict operations."""
        result = WorkResult()
        result["key"] = "value"
        self.assertEqual(result["key"], "value")
        self.assertIn("key", result)

    def test_update_from_dict(self):
        """WorkResult can be updated from another dict."""
        result = WorkResult({"a": 1})
        result.update({"b": 2, "c": 3})
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)
        self.assertEqual(result["c"], 3)


if __name__ == '__main__':
    unittest.main()
