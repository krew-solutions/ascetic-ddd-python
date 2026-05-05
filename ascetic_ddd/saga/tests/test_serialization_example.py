"""Integration tests for the serialization_example module.

Asserts that the runnable example does what its docstring claims, so that
documentation pointing readers at it stays accurate.
"""

import io
import unittest
from contextlib import redirect_stdout

from ascetic_ddd.saga.activity_resolver import MapBasedResolver
from ascetic_ddd.saga.examples.reserve_car_activity import ReserveCarActivity
from ascetic_ddd.saga.examples.reserve_flight_activity import (
    FailingReserveFlightActivity,
    ReserveFlightActivity,
)
from ascetic_ddd.saga.examples.reserve_hotel_activity import ReserveHotelActivity
from ascetic_ddd.saga.examples.serialization_example import (
    make_orchestrator_resolver,
    run_compensation_with_serialization,
    run_travel_booking_with_serialization,
)


class MakeOrchestratorResolverTestCase(unittest.TestCase):
    """make_orchestrator_resolver registers every example activity."""

    def test_registers_all_example_activities(self):
        resolver = make_orchestrator_resolver()

        self.assertIs(resolver.resolve("ReserveCarActivity"), ReserveCarActivity)
        self.assertIs(resolver.resolve("ReserveHotelActivity"), ReserveHotelActivity)
        self.assertIs(resolver.resolve("ReserveFlightActivity"), ReserveFlightActivity)
        self.assertIs(
            resolver.resolve("FailingReserveFlightActivity"),
            FailingReserveFlightActivity,
        )

    def test_returns_fresh_instance_each_call(self):
        """Each call returns an isolated resolver -- no shared global state."""
        a = make_orchestrator_resolver()
        b = make_orchestrator_resolver()

        self.assertIsNot(a, b)
        self.assertIsInstance(a, MapBasedResolver)


class RunTravelBookingTestCase(unittest.IsolatedAsyncioTestCase):
    """Forward-path scenario completes the saga end-to-end."""

    async def test_completes_after_handoff(self):
        with redirect_stdout(io.StringIO()):
            slip = await run_travel_booking_with_serialization()

        self.assertTrue(slip.is_completed)
        self.assertTrue(slip.is_in_progress)
        self.assertEqual(len(slip.completed_work_logs), 3)


class RunCompensationTestCase(unittest.IsolatedAsyncioTestCase):
    """Compensation-path scenario rolls every completed activity back."""

    async def test_compensates_completed_work(self):
        with redirect_stdout(io.StringIO()):
            slip = await run_compensation_with_serialization()

        self.assertFalse(slip.is_in_progress)
        self.assertEqual(len(slip.completed_work_logs), 0)


if __name__ == '__main__':
    unittest.main()
