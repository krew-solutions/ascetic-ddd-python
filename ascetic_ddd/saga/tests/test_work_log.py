"""Tests for WorkLog class."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
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


class WorkLogTestCase(unittest.TestCase):
    """Test cases for WorkLog."""

    def test_create_work_log(self):
        """WorkLog stores activity type and result."""
        activity = StubActivity()
        result = WorkResult({"reservationId": 12345})
        work_log = WorkLog(activity, result)

        self.assertEqual(work_log.activity_type, StubActivity)
        self.assertEqual(work_log.result["reservationId"], 12345)

    def test_result_is_accessible(self):
        """WorkLog result property returns the work result."""
        activity = StubActivity()
        result = WorkResult({"key": "value", "count": 42})
        work_log = WorkLog(activity, result)

        self.assertEqual(work_log.result["key"], "value")
        self.assertEqual(work_log.result["count"], 42)

    def test_activity_type_is_type_not_instance(self):
        """WorkLog stores the activity type, not the instance."""
        activity1 = StubActivity()
        activity2 = StubActivity()
        result = WorkResult()

        work_log = WorkLog(activity1, result)

        self.assertEqual(work_log.activity_type, type(activity2))
        self.assertIs(work_log.activity_type, StubActivity)


if __name__ == '__main__':
    unittest.main()
