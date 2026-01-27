"""Tests for Activity abstract base class."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class ActivityAbstractTestCase(unittest.TestCase):
    """Test cases for Activity abstract class."""

    def test_cannot_instantiate_abstract(self):
        """Activity cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            Activity()

    def test_subclass_must_implement_do_work(self):
        """Subclass must implement do_work()."""
        class IncompleteActivity(Activity):
            def compensate(self, work_log, routing_slip):
                return True

            @property
            def work_item_queue_address(self):
                return "sb://./test"

            @property
            def compensation_queue_address(self):
                return "sb://./testCompensation"

        with self.assertRaises(TypeError):
            IncompleteActivity()

    def test_subclass_must_implement_compensate(self):
        """Subclass must implement compensate()."""
        class IncompleteActivity(Activity):
            def do_work(self, work_item):
                return WorkLog(self, WorkResult())

            @property
            def work_item_queue_address(self):
                return "sb://./test"

            @property
            def compensation_queue_address(self):
                return "sb://./testCompensation"

        with self.assertRaises(TypeError):
            IncompleteActivity()

    def test_subclass_must_implement_work_item_queue_address(self):
        """Subclass must implement work_item_queue_address."""
        class IncompleteActivity(Activity):
            def do_work(self, work_item):
                return WorkLog(self, WorkResult())

            def compensate(self, work_log, routing_slip):
                return True

            @property
            def compensation_queue_address(self):
                return "sb://./testCompensation"

        with self.assertRaises(TypeError):
            IncompleteActivity()

    def test_subclass_must_implement_compensation_queue_address(self):
        """Subclass must implement compensation_queue_address."""
        class IncompleteActivity(Activity):
            def do_work(self, work_item):
                return WorkLog(self, WorkResult())

            def compensate(self, work_log, routing_slip):
                return True

            @property
            def work_item_queue_address(self):
                return "sb://./test"

        with self.assertRaises(TypeError):
            IncompleteActivity()


class ConcreteActivityTestCase(unittest.TestCase):
    """Test cases for concrete Activity implementation."""

    def test_complete_implementation_can_be_instantiated(self):
        """Complete implementation can be instantiated."""
        class CompleteActivity(Activity):
            def do_work(self, work_item: WorkItem) -> WorkLog:
                return WorkLog(self, WorkResult({"done": True}))

            def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
                return True

            @property
            def work_item_queue_address(self) -> str:
                return "sb://./complete"

            @property
            def compensation_queue_address(self) -> str:
                return "sb://./completeCompensation"

        activity = CompleteActivity()
        self.assertIsInstance(activity, Activity)

    def test_do_work_receives_work_item(self):
        """do_work() receives the work item."""
        received_item = None

        class TestActivity(Activity):
            def do_work(self, work_item: WorkItem) -> WorkLog:
                nonlocal received_item
                received_item = work_item
                return WorkLog(self, WorkResult())

            def compensate(self, work_log, routing_slip):
                return True

            @property
            def work_item_queue_address(self):
                return "sb://./test"

            @property
            def compensation_queue_address(self):
                return "sb://./testCompensation"

        activity = TestActivity()
        work_item = WorkItem(TestActivity, WorkItemArguments({"key": "value"}))

        activity.do_work(work_item)

        self.assertIs(received_item, work_item)
        self.assertEqual(received_item.arguments["key"], "value")

    def test_compensate_receives_work_log_and_routing_slip(self):
        """compensate() receives work log and routing slip."""
        received_log = None
        received_slip = None

        class TestActivity(Activity):
            def do_work(self, work_item: WorkItem) -> WorkLog:
                return WorkLog(self, WorkResult({"id": 123}))

            def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
                nonlocal received_log, received_slip
                received_log = work_log
                received_slip = routing_slip
                return True

            @property
            def work_item_queue_address(self):
                return "sb://./test"

            @property
            def compensation_queue_address(self):
                return "sb://./testCompensation"

        activity = TestActivity()
        work_item = WorkItem(TestActivity, WorkItemArguments())
        work_log = activity.do_work(work_item)
        routing_slip = RoutingSlip()

        activity.compensate(work_log, routing_slip)

        self.assertIs(received_log, work_log)
        self.assertIs(received_slip, routing_slip)


if __name__ == '__main__':
    unittest.main()
