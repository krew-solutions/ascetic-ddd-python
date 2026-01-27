"""Tests for WorkItemArguments class."""

import unittest

from ascetic_ddd.saga.work_item_arguments import WorkItemArguments


class WorkItemArgumentsTestCase(unittest.TestCase):
    """Test cases for WorkItemArguments."""

    def test_create_empty(self):
        """WorkItemArguments can be created empty."""
        args = WorkItemArguments()
        self.assertEqual(len(args), 0)

    def test_create_with_data(self):
        """WorkItemArguments can be created with initial data."""
        args = WorkItemArguments({"vehicleType": "Compact", "days": 5})
        self.assertEqual(args["vehicleType"], "Compact")
        self.assertEqual(args["days"], 5)

    def test_is_dict_subclass(self):
        """WorkItemArguments is a dict subclass."""
        args = WorkItemArguments()
        self.assertIsInstance(args, dict)

    def test_set_and_get_items(self):
        """WorkItemArguments supports dict operations."""
        args = WorkItemArguments()
        args["destination"] = "Paris"
        self.assertEqual(args["destination"], "Paris")
        self.assertIn("destination", args)

    def test_missing_key_raises_error(self):
        """Accessing missing key raises KeyError."""
        args = WorkItemArguments({"a": 1})
        with self.assertRaises(KeyError):
            _ = args["missing"]


if __name__ == '__main__':
    unittest.main()
