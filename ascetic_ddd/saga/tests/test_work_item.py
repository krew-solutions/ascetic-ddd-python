"""Tests for WorkItem class."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class StubActivity(Activity):
    """Stub activity for testing."""

    def do_work(self, work_item: WorkItem) -> WorkLog:
        return WorkLog(self, WorkResult({"id": 123}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./stub"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./stubCompensation"


class WorkItemTestCase(unittest.TestCase):
    """Test cases for WorkItem."""

    def test_create_work_item(self):
        """WorkItem stores activity type and arguments."""
        args = WorkItemArguments({"vehicleType": "SUV"})
        work_item = WorkItem(StubActivity, args)

        self.assertEqual(work_item.activity_type, StubActivity)
        self.assertEqual(work_item.arguments["vehicleType"], "SUV")

    def test_routing_slip_initially_none(self):
        """WorkItem routing_slip is None initially."""
        args = WorkItemArguments()
        work_item = WorkItem(StubActivity, args)

        self.assertIsNone(work_item.routing_slip)

    def test_routing_slip_can_be_set(self):
        """WorkItem routing_slip can be assigned."""
        args = WorkItemArguments()
        work_item = WorkItem(StubActivity, args)
        routing_slip = RoutingSlip()

        work_item.routing_slip = routing_slip

        self.assertIs(work_item.routing_slip, routing_slip)

    def test_arguments_are_accessible(self):
        """WorkItem arguments are accessible."""
        args = WorkItemArguments({"a": 1, "b": 2, "c": 3})
        work_item = WorkItem(StubActivity, args)

        self.assertEqual(work_item.arguments["a"], 1)
        self.assertEqual(work_item.arguments["b"], 2)
        self.assertEqual(work_item.arguments["c"], 3)


if __name__ == '__main__':
    unittest.main()
