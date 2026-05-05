"""Tests for example activities."""

import unittest

from ascetic_ddd.saga.activity_resolver import NamedActivity
from ascetic_ddd.saga.examples import (
    FailingReserveFlightActivity,
    ReserveCarActivity,
    ReserveFlightActivity,
    ReserveHotelActivity,
)
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments


class ReserveCarActivityTestCase(unittest.IsolatedAsyncioTestCase):
    """Test cases for ReserveCarActivity."""

    async def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveCarActivity()
        work_item = WorkItem(
            ReserveCarActivity,
            WorkItemArguments({"vehicleType": "Compact"})
        )

        result = await activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    async def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveCarActivity()
        work_item = WorkItem(
            ReserveCarActivity,
            WorkItemArguments({"vehicleType": "SUV"})
        )
        work_log = await activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = await activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveCarActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./carReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./carCancellations")


class ReserveHotelActivityTestCase(unittest.IsolatedAsyncioTestCase):
    """Test cases for ReserveHotelActivity."""

    async def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveHotelActivity()
        work_item = WorkItem(
            ReserveHotelActivity,
            WorkItemArguments({"roomType": "Suite"})
        )

        result = await activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    async def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveHotelActivity()
        work_item = WorkItem(
            ReserveHotelActivity,
            WorkItemArguments({"roomType": "Standard"})
        )
        work_log = await activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = await activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveHotelActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./hotelReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./hotelCancellations")


class ReserveFlightActivityTestCase(unittest.IsolatedAsyncioTestCase):
    """Test cases for ReserveFlightActivity."""

    async def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveFlightActivity()
        work_item = WorkItem(
            ReserveFlightActivity,
            WorkItemArguments({"destination": "DUS"})
        )

        result = await activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    async def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveFlightActivity()
        work_item = WorkItem(
            ReserveFlightActivity,
            WorkItemArguments({"destination": "FRA"})
        )
        work_log = await activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = await activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveFlightActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./flightReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./flightCancellations")


class FailingReserveFlightActivityTestCase(unittest.IsolatedAsyncioTestCase):
    """Test cases for FailingReserveFlightActivity."""

    async def test_do_work_raises_key_error(self):
        """do_work() raises KeyError due to missing key."""
        activity = FailingReserveFlightActivity()
        work_item = WorkItem(
            FailingReserveFlightActivity,
            WorkItemArguments({"destination": "DUS"})
        )

        with self.assertRaises(KeyError):
            await activity.do_work(work_item)

    def test_inherits_queue_addresses(self):
        """Inherits queue addresses from parent."""
        activity = FailingReserveFlightActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./flightReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./flightCancellations")


class NamedActivityProtocolTestCase(unittest.TestCase):
    """Each example activity exposes its canonical name via NamedActivity."""

    def test_reserve_car_activity_is_named(self):
        activity = ReserveCarActivity()

        self.assertIsInstance(activity, NamedActivity)
        self.assertEqual(activity.type_name(), "ReserveCarActivity")

    def test_reserve_hotel_activity_is_named(self):
        activity = ReserveHotelActivity()

        self.assertIsInstance(activity, NamedActivity)
        self.assertEqual(activity.type_name(), "ReserveHotelActivity")

    def test_reserve_flight_activity_is_named(self):
        activity = ReserveFlightActivity()

        self.assertIsInstance(activity, NamedActivity)
        self.assertEqual(activity.type_name(), "ReserveFlightActivity")

    def test_failing_reserve_flight_activity_inherits_name(self):
        """FailingReserveFlightActivity inherits type_name() from its parent."""
        activity = FailingReserveFlightActivity()

        self.assertIsInstance(activity, NamedActivity)
        self.assertEqual(activity.type_name(), "ReserveFlightActivity")


class TravelBookingSagaTestCase(unittest.IsolatedAsyncioTestCase):
    """Integration tests for the travel booking saga."""

    async def test_successful_booking(self):
        """All reservations succeed."""
        slip = RoutingSlip([
            WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
            WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
            WorkItem(ReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
        ])

        while not slip.is_completed:
            result = await slip.process_next()
            self.assertTrue(result)

        self.assertTrue(slip.is_completed)
        self.assertEqual(len(slip.completed_work_logs), 3)

    async def test_failed_booking_triggers_compensation(self):
        """Failed flight triggers compensation of car and hotel."""
        slip = RoutingSlip([
            WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
            WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
            WorkItem(FailingReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
        ])

        # Process until failure
        completed_before_failure = 0
        while not slip.is_completed:
            if await slip.process_next():
                completed_before_failure += 1
            else:
                break

        self.assertEqual(completed_before_failure, 2)

        # Compensate
        compensated = 0
        while slip.is_in_progress:
            await slip.undo_last()
            compensated += 1

        self.assertEqual(compensated, 2)
        self.assertFalse(slip.is_in_progress)


if __name__ == '__main__':
    unittest.main()
