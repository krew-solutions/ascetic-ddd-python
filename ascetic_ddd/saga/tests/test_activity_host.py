"""Tests for ActivityHost class."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.activity_host import ActivityHost
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class Activity1(Activity):
    """First test activity."""

    call_count = 0
    compensate_count = 0

    def do_work(self, work_item: WorkItem) -> WorkLog:
        Activity1.call_count += 1
        return WorkLog(self, WorkResult({"id": Activity1.call_count}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        Activity1.compensate_count += 1
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./activity1"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./activity1Compensation"


class Activity2(Activity):
    """Second test activity."""

    call_count = 0
    compensate_count = 0

    def do_work(self, work_item: WorkItem) -> WorkLog:
        Activity2.call_count += 1
        return WorkLog(self, WorkResult({"id": Activity2.call_count}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        Activity2.compensate_count += 1
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./activity2"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./activity2Compensation"


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


class ActivityHostAcceptMessageTestCase(unittest.TestCase):
    """Test cases for ActivityHost.accept_message()."""

    def setUp(self):
        Activity1.call_count = 0
        Activity1.compensate_count = 0
        Activity2.call_count = 0
        Activity2.compensate_count = 0
        self.sent_messages = []

    def send(self, uri: str, routing_slip: RoutingSlip):
        self.sent_messages.append((uri, routing_slip))

    def test_accept_work_item_message(self):
        """Host accepts message for its work queue."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([WorkItem(Activity1, WorkItemArguments())])

        result = host.accept_message("sb://./activity1", slip)

        self.assertTrue(result)

    def test_accept_compensation_message(self):
        """Host accepts message for its compensation queue."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([WorkItem(Activity1, WorkItemArguments())])
        slip.process_next()

        result = host.accept_message("sb://./activity1Compensation", slip)

        self.assertTrue(result)

    def test_reject_unknown_message(self):
        """Host rejects message for unknown queue."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([WorkItem(Activity1, WorkItemArguments())])

        result = host.accept_message("sb://./unknown", slip)

        self.assertFalse(result)

    def test_reject_other_activity_message(self):
        """Host rejects message for other activity."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([WorkItem(Activity2, WorkItemArguments())])

        result = host.accept_message("sb://./activity2", slip)

        self.assertFalse(result)


class ActivityHostForwardMessageTestCase(unittest.TestCase):
    """Test cases for ActivityHost.process_forward_message()."""

    def setUp(self):
        Activity1.call_count = 0
        Activity1.compensate_count = 0
        Activity2.call_count = 0
        Activity2.compensate_count = 0
        self.sent_messages = []

    def send(self, uri: str, routing_slip: RoutingSlip):
        self.sent_messages.append((uri, routing_slip))

    def test_forward_success_continues_forward(self):
        """Successful work sends to next activity."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([
            WorkItem(Activity1, WorkItemArguments()),
            WorkItem(Activity2, WorkItemArguments()),
        ])

        host.process_forward_message(slip)

        self.assertEqual(len(self.sent_messages), 1)
        uri, _ = self.sent_messages[0]
        self.assertEqual(uri, "sb://./activity2")

    def test_forward_failure_starts_compensation(self):
        """Failed work sends to compensation queue."""
        host = ActivityHost(FailingActivity, self.send)
        slip = RoutingSlip([
            WorkItem(Activity1, WorkItemArguments()),
            WorkItem(FailingActivity, WorkItemArguments()),
        ])
        slip.process_next()  # Complete Activity1

        host.process_forward_message(slip)

        self.assertEqual(len(self.sent_messages), 1)
        uri, _ = self.sent_messages[0]
        self.assertEqual(uri, "sb://./activity1Compensation")

    def test_forward_completed_does_nothing(self):
        """Forward on completed slip does nothing."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip()

        host.process_forward_message(slip)

        self.assertEqual(len(self.sent_messages), 0)


class ActivityHostBackwardMessageTestCase(unittest.TestCase):
    """Test cases for ActivityHost.process_backward_message()."""

    def setUp(self):
        Activity1.call_count = 0
        Activity1.compensate_count = 0
        Activity2.call_count = 0
        Activity2.compensate_count = 0
        self.sent_messages = []

    def send(self, uri: str, routing_slip: RoutingSlip):
        self.sent_messages.append((uri, routing_slip))

    def test_backward_continues_backward(self):
        """Compensation continues to previous activity."""
        host = ActivityHost(Activity2, self.send)
        slip = RoutingSlip([
            WorkItem(Activity1, WorkItemArguments()),
            WorkItem(Activity2, WorkItemArguments()),
        ])
        slip.process_next()
        slip.process_next()

        host.process_backward_message(slip)

        self.assertEqual(len(self.sent_messages), 1)
        uri, _ = self.sent_messages[0]
        self.assertEqual(uri, "sb://./activity1Compensation")

    def test_backward_not_in_progress_does_nothing(self):
        """Backward on non-started slip does nothing."""
        host = ActivityHost(Activity1, self.send)
        slip = RoutingSlip([WorkItem(Activity1, WorkItemArguments())])

        host.process_backward_message(slip)

        self.assertEqual(len(self.sent_messages), 0)


class ActivityHostFullSagaTestCase(unittest.TestCase):
    """Integration tests for full saga with multiple hosts."""

    def setUp(self):
        Activity1.call_count = 0
        Activity1.compensate_count = 0
        Activity2.call_count = 0
        Activity2.compensate_count = 0

    def test_distributed_saga_success(self):
        """Saga completes through multiple hosts."""
        messages = []

        def send(uri: str, routing_slip: RoutingSlip):
            messages.append((uri, routing_slip))

        host1 = ActivityHost(Activity1, send)
        host2 = ActivityHost(Activity2, send)
        hosts = [host1, host2]

        slip = RoutingSlip([
            WorkItem(Activity1, WorkItemArguments()),
            WorkItem(Activity2, WorkItemArguments()),
        ])

        # Start saga
        send(slip.progress_uri, slip)

        # Process all messages
        while messages:
            uri, routing_slip = messages.pop(0)
            for host in hosts:
                if host.accept_message(uri, routing_slip):
                    break

        self.assertTrue(slip.is_completed)
        self.assertEqual(Activity1.call_count, 1)
        self.assertEqual(Activity2.call_count, 1)

    def test_distributed_saga_with_compensation(self):
        """Failed saga compensates through hosts."""
        messages = []

        def send(uri: str, routing_slip: RoutingSlip):
            messages.append((uri, routing_slip))

        host1 = ActivityHost(Activity1, send)
        host2 = ActivityHost(Activity2, send)
        host_fail = ActivityHost(FailingActivity, send)
        hosts = [host1, host2, host_fail]

        slip = RoutingSlip([
            WorkItem(Activity1, WorkItemArguments()),
            WorkItem(Activity2, WorkItemArguments()),
            WorkItem(FailingActivity, WorkItemArguments()),
        ])

        # Start saga
        send(slip.progress_uri, slip)

        # Process all messages
        while messages:
            uri, routing_slip = messages.pop(0)
            for host in hosts:
                if host.accept_message(uri, routing_slip):
                    break

        self.assertFalse(slip.is_in_progress)
        self.assertEqual(Activity1.call_count, 1)
        self.assertEqual(Activity2.call_count, 1)
        self.assertEqual(Activity1.compensate_count, 1)
        self.assertEqual(Activity2.compensate_count, 1)


if __name__ == '__main__':
    unittest.main()
