"""Tests for RoutingSlip <-> SerializableRoutingSlip conversion."""

import json
import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.activity_resolver import MapBasedResolver
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.routing_slip_serialization import (
    from_serializable,
    to_serializable,
)
from ascetic_ddd.saga.serializable_routing_slip import (
    SerializableRoutingSlip,
    SerializableWorkItem,
    SerializableWorkLog,
)
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class SerializableSuccessActivity(Activity):
    """Activity that always succeeds and reports its type name."""

    call_count = 0
    compensate_count = 0

    async def do_work(self, work_item: WorkItem) -> WorkLog:
        SerializableSuccessActivity.call_count += 1
        return WorkLog(
            self,
            WorkResult({"id": SerializableSuccessActivity.call_count}),
        )

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        SerializableSuccessActivity.compensate_count += 1
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./success"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./successCompensation"

    def type_name(self) -> str:
        return "SerializableSuccessActivity"


class AnonymousActivity(Activity):
    """Activity without NamedActivity protocol implementation."""

    async def do_work(self, work_item: WorkItem) -> WorkLog:
        return WorkLog(self, WorkResult())

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./anon"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./anonCompensation"


def _reset_counters() -> None:
    SerializableSuccessActivity.call_count = 0
    SerializableSuccessActivity.compensate_count = 0


class ToSerializableTestCase(unittest.IsolatedAsyncioTestCase):
    """to_serializable() converts a RoutingSlip into name-keyed form."""

    def setUp(self) -> None:
        _reset_counters()

    def test_empty_slip(self):
        """An empty slip produces empty lists."""
        resolver = MapBasedResolver()
        slip = RoutingSlip()

        serializable = to_serializable(slip, resolver)

        self.assertEqual(serializable.completed_work_logs, [])
        self.assertEqual(serializable.next_work_items, [])

    def test_pending_items_only(self):
        """Pending work items become SerializableWorkItem entries."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        slip = RoutingSlip([
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"a": 1})),
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"b": 2})),
        ])

        serializable = to_serializable(slip, resolver)

        self.assertEqual(len(serializable.next_work_items), 2)
        self.assertEqual(
            serializable.next_work_items[0].activity_type_name,
            "SerializableSuccessActivity",
        )
        self.assertEqual(serializable.next_work_items[0].arguments, {"a": 1})
        self.assertEqual(serializable.next_work_items[1].arguments, {"b": 2})

    async def test_completed_work_logs(self):
        """Completed work logs become SerializableWorkLog entries."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        slip = RoutingSlip([
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"x": "test"})),
        ])
        await slip.process_next()

        serializable = to_serializable(slip, resolver)

        self.assertEqual(len(serializable.completed_work_logs), 1)
        self.assertEqual(
            serializable.completed_work_logs[0].activity_type_name,
            "SerializableSuccessActivity",
        )
        self.assertEqual(serializable.completed_work_logs[0].result["id"], 1)

    def test_unregistered_anonymous_activity_raises(self):
        """Activity that is neither registered nor NamedActivity cannot be serialized."""
        resolver = MapBasedResolver()
        slip = RoutingSlip([
            WorkItem(AnonymousActivity, WorkItemArguments()),
        ])

        with self.assertRaises(KeyError):
            to_serializable(slip, resolver)

    def test_unregistered_named_activity_falls_back(self):
        """Unregistered NamedActivity is serializable via type_name() fallback."""
        resolver = MapBasedResolver()
        slip = RoutingSlip([
            WorkItem(SerializableSuccessActivity, WorkItemArguments()),
        ])

        # Intentionally not registered.
        serializable = to_serializable(slip, resolver)

        self.assertEqual(
            serializable.next_work_items[0].activity_type_name,
            "SerializableSuccessActivity",
        )


class FromSerializableTestCase(unittest.IsolatedAsyncioTestCase):
    """from_serializable() reconstructs a RoutingSlip from name-keyed form."""

    def setUp(self) -> None:
        _reset_counters()

    def test_empty_serializable(self):
        """Empty serializable form yields a completed slip."""
        resolver = MapBasedResolver()
        serializable = SerializableRoutingSlip()

        slip = from_serializable(serializable, resolver)

        self.assertTrue(slip.is_completed)
        self.assertFalse(slip.is_in_progress)

    def test_pending_items_are_restored(self):
        """next_work_items entries become pending WorkItems in order."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        serializable = SerializableRoutingSlip(
            next_work_items=[
                SerializableWorkItem("SerializableSuccessActivity", WorkItemArguments({"a": 1})),
                SerializableWorkItem("SerializableSuccessActivity", WorkItemArguments({"b": 2})),
            ],
        )

        slip = from_serializable(serializable, resolver)

        self.assertFalse(slip.is_completed)
        self.assertEqual(len(slip.pending_work_items), 2)
        self.assertEqual(slip.pending_work_items[0].arguments, {"a": 1})
        self.assertEqual(slip.pending_work_items[1].arguments, {"b": 2})

    def test_completed_work_logs_are_restored(self):
        """completed_work_logs entries become recorded WorkLogs."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        serializable = SerializableRoutingSlip(
            completed_work_logs=[
                SerializableWorkLog("SerializableSuccessActivity", WorkResult({"id": 42})),
            ],
        )

        slip = from_serializable(serializable, resolver)

        self.assertTrue(slip.is_in_progress)
        self.assertEqual(len(slip.completed_work_logs), 1)
        self.assertEqual(slip.completed_work_logs[0].result["id"], 42)
        self.assertIs(
            slip.completed_work_logs[0].activity_type,
            SerializableSuccessActivity,
        )

    def test_unregistered_activity_raises(self):
        """Unknown activity name causes KeyError on deserialization."""
        resolver = MapBasedResolver()
        serializable = SerializableRoutingSlip(
            next_work_items=[
                SerializableWorkItem("UnregisteredActivity", WorkItemArguments()),
            ],
        )

        with self.assertRaises(KeyError):
            from_serializable(serializable, resolver)


class RoundTripTestCase(unittest.IsolatedAsyncioTestCase):
    """End-to-end serialize -> JSON -> deserialize -> resume."""

    def setUp(self) -> None:
        _reset_counters()

    async def test_state_is_preserved(self):
        """Restored slip has the same completed and pending work."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        original = RoutingSlip([
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"step": 1})),
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"step": 2})),
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"step": 3})),
        ])
        await original.process_next()

        serializable = to_serializable(original, resolver)
        restored = from_serializable(serializable, resolver)

        self.assertEqual(len(restored.completed_work_logs), 1)
        self.assertEqual(len(restored.pending_work_items), 2)

        # Continue processing the restored slip.
        await restored.process_next()
        await restored.process_next()
        self.assertTrue(restored.is_completed)
        self.assertEqual(len(restored.completed_work_logs), 3)

    async def test_round_trip_through_json(self):
        """Round-trip through JSON preserves the slip state."""
        resolver = MapBasedResolver()
        resolver.register("SerializableSuccessActivity", SerializableSuccessActivity)
        original = RoutingSlip([
            WorkItem(SerializableSuccessActivity, WorkItemArguments({"key": "value"})),
        ])
        await original.process_next()

        wire = json.dumps(to_serializable(original, resolver).to_dict())
        restored = from_serializable(
            SerializableRoutingSlip.from_dict(json.loads(wire)),
            resolver,
        )

        self.assertEqual(len(restored.completed_work_logs), 1)
        self.assertEqual(restored.completed_work_logs[0].result["id"], 1)


class JsonWireFormatTestCase(unittest.TestCase):
    """to_dict() / from_dict() use camelCase keys for cross-language interop."""

    def test_to_dict_uses_camel_case(self):
        """Top-level and nested keys are camelCase."""
        serializable = SerializableRoutingSlip(
            completed_work_logs=[
                SerializableWorkLog("ActivityA", WorkResult({"id": 1})),
            ],
            next_work_items=[
                SerializableWorkItem("ActivityB", WorkItemArguments({"k": "v"})),
            ],
        )

        wire = serializable.to_dict()

        self.assertEqual(set(wire.keys()), {"completedWorkLogs", "nextWorkItems"})
        self.assertEqual(
            set(wire["completedWorkLogs"][0].keys()),
            {"activityTypeName", "result"},
        )
        self.assertEqual(
            set(wire["nextWorkItems"][0].keys()),
            {"activityTypeName", "arguments"},
        )

    def test_from_dict_round_trip(self):
        """from_dict(to_dict(x)) preserves all fields."""
        original = SerializableRoutingSlip(
            completed_work_logs=[
                SerializableWorkLog("A", WorkResult({"x": 1})),
            ],
            next_work_items=[
                SerializableWorkItem("B", WorkItemArguments({"y": 2})),
            ],
        )

        restored = SerializableRoutingSlip.from_dict(original.to_dict())

        self.assertEqual(restored, original)

    def test_from_dict_missing_keys_default_to_empty(self):
        """Missing top-level keys produce empty lists, not errors."""
        restored = SerializableRoutingSlip.from_dict({})

        self.assertEqual(restored.completed_work_logs, [])
        self.assertEqual(restored.next_work_items, [])


if __name__ == '__main__':
    unittest.main()
