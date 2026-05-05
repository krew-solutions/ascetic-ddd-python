"""Tests for ActivityTypeResolver and MapBasedResolver."""

import unittest

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.activity_resolver import (
    MapBasedResolver,
    NamedActivity,
)
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


class TestNamedActivity(Activity):
    """Activity that exposes an explicit type_name (NamedActivity protocol)."""

    async def do_work(self, work_item: WorkItem) -> WorkLog:
        return WorkLog(self, WorkResult())

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./test"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./testCompensation"

    def type_name(self) -> str:
        return "TestNamedActivity"


class AnotherNamedActivity(Activity):
    """Second NamedActivity for multi-registration tests."""

    async def do_work(self, work_item: WorkItem) -> WorkLog:
        return WorkLog(self, WorkResult())

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        return True

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./another"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./anotherCompensation"

    def type_name(self) -> str:
        return "AnotherNamedActivity"


class AnonymousActivity(Activity):
    """Activity that does NOT implement NamedActivity (no type_name method)."""

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


class MapBasedResolverRegisterAndResolveTestCase(unittest.TestCase):
    """register() / resolve() round-trip."""

    def test_register_and_resolve(self):
        """Resolved activity class equals the registered one."""
        resolver = MapBasedResolver()
        resolver.register("TestNamedActivity", TestNamedActivity)

        resolved = resolver.resolve("TestNamedActivity")

        self.assertIs(resolved, TestNamedActivity)
        self.assertIsInstance(resolved(), TestNamedActivity)

    def test_resolve_unregistered_type_raises(self):
        """resolve() on unknown name raises KeyError."""
        resolver = MapBasedResolver()

        with self.assertRaises(KeyError):
            resolver.resolve("UnregisteredActivity")

    def test_multiple_registrations(self):
        """Multiple distinct registrations resolve independently."""
        resolver = MapBasedResolver()
        resolver.register("TestNamedActivity", TestNamedActivity)
        resolver.register("AnotherNamedActivity", AnotherNamedActivity)

        self.assertIs(resolver.resolve("TestNamedActivity"), TestNamedActivity)
        self.assertIs(resolver.resolve("AnotherNamedActivity"), AnotherNamedActivity)

    def test_register_overwrite(self):
        """Re-registering the same name replaces the previous mapping."""
        resolver = MapBasedResolver()
        resolver.register("TestActivity", TestNamedActivity)
        resolver.register("TestActivity", AnotherNamedActivity)

        self.assertIs(resolver.resolve("TestActivity"), AnotherNamedActivity)


class MapBasedResolverGetNameTestCase(unittest.TestCase):
    """get_name() lookups, including the NamedActivity fallback."""

    def test_get_name_for_registered_type(self):
        """get_name() returns the registered name."""
        resolver = MapBasedResolver()
        resolver.register("TestNamedActivity", TestNamedActivity)

        self.assertEqual(resolver.get_name(TestNamedActivity), "TestNamedActivity")

    def test_get_name_falls_back_to_named_activity(self):
        """For unregistered NamedActivity, type_name() is used as fallback."""
        resolver = MapBasedResolver()

        # Intentionally not registered.
        self.assertEqual(resolver.get_name(TestNamedActivity), "TestNamedActivity")

    def test_get_name_unregistered_anonymous_raises(self):
        """Activity that is neither registered nor named raises KeyError."""
        resolver = MapBasedResolver()

        with self.assertRaises(KeyError):
            resolver.get_name(AnonymousActivity)


class MapBasedResolverIsolationTestCase(unittest.TestCase):
    """Each resolver is independent — no shared global state."""

    def test_isolated_instances(self):
        """Registration in one resolver does not leak into another."""
        resolver_a = MapBasedResolver()
        resolver_b = MapBasedResolver()
        resolver_a.register("TestNamedActivity", TestNamedActivity)

        self.assertIs(resolver_a.resolve("TestNamedActivity"), TestNamedActivity)
        with self.assertRaises(KeyError):
            resolver_b.resolve("TestNamedActivity")


class NamedActivityProtocolTestCase(unittest.TestCase):
    """NamedActivity is a runtime-checkable Protocol."""

    def test_named_activity_satisfies_protocol(self):
        """An instance providing type_name() passes isinstance(NamedActivity)."""
        self.assertIsInstance(TestNamedActivity(), NamedActivity)

    def test_anonymous_activity_does_not_satisfy_protocol(self):
        """An instance without type_name() fails isinstance(NamedActivity)."""
        self.assertNotIsInstance(AnonymousActivity(), NamedActivity)


if __name__ == '__main__':
    unittest.main()
