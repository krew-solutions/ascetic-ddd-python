"""Tests for RoutingSlip class."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import InvalidOperationError, RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class SuccessActivity(Activity):
    """Activity that always succeeds."""

    call_count = 0
    compensate_count = 0

    def do_work(self, work_item: WorkItem) -> WorkLog:
        SuccessActivity.call_count += 1
        return WorkLog(self, WorkResult({"id": SuccessActivity.call_count}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        SuccessActivity.compensate_count += 1
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./success"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./successCompensation"


class FailingActivity(Activity):
    """Activity that always fails."""

    def do_work(self, work_item: WorkItem) -> WorkLog:
        raise RuntimeError("Intentional failure")

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./failing"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./failingCompensation"


class RoutingSlipCreationTestCase(unittest.TestCase):
    """Test cases for RoutingSlip creation."""

    def test_create_empty(self):
        """RoutingSlip can be created empty."""
        slip = RoutingSlip()
        self.assertTrue(slip.is_completed)
        self.assertFalse(slip.is_in_progress)

    def test_create_with_work_items(self):
        """RoutingSlip can be created with work items."""
        work_items = [
            WorkItem(SuccessActivity, WorkItemArguments({"a": 1})),
            WorkItem(SuccessActivity, WorkItemArguments({"b": 2})),
        ]
        slip = RoutingSlip(work_items)

        self.assertFalse(slip.is_completed)
        self.assertFalse(slip.is_in_progress)


class RoutingSlipProcessNextTestCase(unittest.TestCase):
    """Test cases for RoutingSlip.process_next()."""

    def setUp(self):
        SuccessActivity.call_count = 0
        SuccessActivity.compensate_count = 0

    def test_process_next_success(self):
        """process_next() returns True on success."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        result = slip.process_next()

        self.assertTrue(result)
        self.assertTrue(slip.is_completed)
        self.assertTrue(slip.is_in_progress)

    def test_process_next_failure(self):
        """process_next() returns False on failure."""
        slip = RoutingSlip([
            WorkItem(FailingActivity, WorkItemArguments()),
        ])

        result = slip.process_next()

        self.assertFalse(result)
        self.assertTrue(slip.is_completed)
        self.assertFalse(slip.is_in_progress)

    def test_process_next_on_empty_raises_error(self):
        """process_next() on completed slip raises error."""
        slip = RoutingSlip()

        with self.assertRaises(InvalidOperationError):
            slip.process_next()

    def test_process_multiple_items(self):
        """process_next() processes items in order."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        slip.process_next()
        self.assertFalse(slip.is_completed)
        self.assertEqual(len(slip.completed_work_logs), 1)

        slip.process_next()
        self.assertFalse(slip.is_completed)
        self.assertEqual(len(slip.completed_work_logs), 2)

        slip.process_next()
        self.assertTrue(slip.is_completed)
        self.assertEqual(len(slip.completed_work_logs), 3)


class RoutingSlipUndoLastTestCase(unittest.TestCase):
    """Test cases for RoutingSlip.undo_last()."""

    def setUp(self):
        SuccessActivity.call_count = 0
        SuccessActivity.compensate_count = 0

    def test_undo_last_success(self):
        """undo_last() compensates last completed work."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])
        slip.process_next()

        result = slip.undo_last()

        self.assertTrue(result)
        self.assertFalse(slip.is_in_progress)
        self.assertEqual(SuccessActivity.compensate_count, 1)

    def test_undo_last_on_empty_raises_error(self):
        """undo_last() on non-started slip raises error."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        with self.assertRaises(InvalidOperationError):
            slip.undo_last()

    def test_undo_multiple_items(self):
        """undo_last() compensates in reverse order."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])
        slip.process_next()
        slip.process_next()
        slip.process_next()

        self.assertEqual(len(slip.completed_work_logs), 3)

        slip.undo_last()
        self.assertEqual(len(slip.completed_work_logs), 2)

        slip.undo_last()
        self.assertEqual(len(slip.completed_work_logs), 1)

        slip.undo_last()
        self.assertEqual(len(slip.completed_work_logs), 0)
        self.assertFalse(slip.is_in_progress)


class RoutingSlipUriTestCase(unittest.TestCase):
    """Test cases for RoutingSlip URI properties."""

    def test_progress_uri_returns_next_activity_queue(self):
        """progress_uri returns next activity's work queue."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        self.assertEqual(slip.progress_uri, "sb://./success")

    def test_progress_uri_returns_none_when_completed(self):
        """progress_uri returns None when completed."""
        slip = RoutingSlip()

        self.assertIsNone(slip.progress_uri)

    def test_compensation_uri_returns_last_activity_queue(self):
        """compensation_uri returns last completed activity's compensation queue."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])
        slip.process_next()

        self.assertEqual(slip.compensation_uri, "sb://./successCompensation")

    def test_compensation_uri_returns_none_when_not_started(self):
        """compensation_uri returns None when no work completed."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        self.assertIsNone(slip.compensation_uri)


class RoutingSlipFullSagaTestCase(unittest.TestCase):
    """Integration tests for full saga execution."""

    def setUp(self):
        SuccessActivity.call_count = 0
        SuccessActivity.compensate_count = 0

    def test_successful_saga(self):
        """Full saga completes successfully."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
        ])

        while not slip.is_completed:
            slip.process_next()

        self.assertTrue(slip.is_completed)
        self.assertTrue(slip.is_in_progress)
        self.assertEqual(len(slip.completed_work_logs), 3)

    def test_failed_saga_with_compensation(self):
        """Failed saga triggers compensation."""
        slip = RoutingSlip([
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(SuccessActivity, WorkItemArguments()),
            WorkItem(FailingActivity, WorkItemArguments()),
        ])

        # Process until failure
        while not slip.is_completed:
            if not slip.process_next():
                break

        # Compensate
        while slip.is_in_progress:
            slip.undo_last()

        self.assertFalse(slip.is_in_progress)
        self.assertEqual(SuccessActivity.compensate_count, 2)


if __name__ == '__main__':
    unittest.main()
