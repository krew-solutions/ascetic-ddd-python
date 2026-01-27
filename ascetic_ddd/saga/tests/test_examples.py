"""Tests for example activities."""

import unittest

from ascetic_ddd.saga.examples import (
    FailingReserveFlightActivity,
    ReserveCarActivity,
    ReserveFlightActivity,
    ReserveHotelActivity,
)
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments


class ReserveCarActivityTestCase(unittest.TestCase):
    """Test cases for ReserveCarActivity."""

    def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveCarActivity()
        work_item = WorkItem(
            ReserveCarActivity,
            WorkItemArguments({"vehicleType": "Compact"})
        )

        result = activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveCarActivity()
        work_item = WorkItem(
            ReserveCarActivity,
            WorkItemArguments({"vehicleType": "SUV"})
        )
        work_log = activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveCarActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./carReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./carCancellations")


class ReserveHotelActivityTestCase(unittest.TestCase):
    """Test cases for ReserveHotelActivity."""

    def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveHotelActivity()
        work_item = WorkItem(
            ReserveHotelActivity,
            WorkItemArguments({"roomType": "Suite"})
        )

        result = activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveHotelActivity()
        work_item = WorkItem(
            ReserveHotelActivity,
            WorkItemArguments({"roomType": "Standard"})
        )
        work_log = activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveHotelActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./hotelReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./hotelCancellations")


class ReserveFlightActivityTestCase(unittest.TestCase):
    """Test cases for ReserveFlightActivity."""

    def test_do_work_creates_reservation(self):
        """do_work() creates a reservation with ID."""
        activity = ReserveFlightActivity()
        work_item = WorkItem(
            ReserveFlightActivity,
            WorkItemArguments({"destination": "DUS"})
        )

        result = activity.do_work(work_item)

        self.assertIn("reservationId", result.result)
        self.assertIsInstance(result.result["reservationId"], int)

    def test_compensate_returns_true(self):
        """compensate() returns True to continue backward."""
        activity = ReserveFlightActivity()
        work_item = WorkItem(
            ReserveFlightActivity,
            WorkItemArguments({"destination": "FRA"})
        )
        work_log = activity.do_work(work_item)
        routing_slip = RoutingSlip()

        result = activity.compensate(work_log, routing_slip)

        self.assertTrue(result)

    def test_queue_addresses(self):
        """Activity has correct queue addresses."""
        activity = ReserveFlightActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./flightReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./flightCancellations")


class FailingReserveFlightActivityTestCase(unittest.TestCase):
    """Test cases for FailingReserveFlightActivity."""

    def test_do_work_raises_key_error(self):
        """do_work() raises KeyError due to missing key."""
        activity = FailingReserveFlightActivity()
        work_item = WorkItem(
            FailingReserveFlightActivity,
            WorkItemArguments({"destination": "DUS"})
        )

        with self.assertRaises(KeyError):
            activity.do_work(work_item)

    def test_inherits_queue_addresses(self):
        """Inherits queue addresses from parent."""
        activity = FailingReserveFlightActivity()

        self.assertEqual(activity.work_item_queue_address, "sb://./flightReservations")
        self.assertEqual(activity.compensation_queue_address, "sb://./flightCancellations")


class TravelBookingSagaTestCase(unittest.TestCase):
    """Integration tests for the travel booking saga."""

    def test_successful_booking(self):
        """All reservations succeed."""
        slip = RoutingSlip([
            WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
            WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
            WorkItem(ReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
        ])

        while not slip.is_completed:
            result = slip.process_next()
            self.assertTrue(result)

        self.assertTrue(slip.is_completed)
        self.assertEqual(len(slip.completed_work_logs), 3)

    def test_failed_booking_triggers_compensation(self):
        """Failed flight triggers compensation of car and hotel."""
        slip = RoutingSlip([
            WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
            WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
            WorkItem(FailingReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
        ])

        # Process until failure
        completed_before_failure = 0
        while not slip.is_completed:
            if slip.process_next():
                completed_before_failure += 1
            else:
                break

        self.assertEqual(completed_before_failure, 2)

        # Compensate
        compensated = 0
        while slip.is_in_progress:
            slip.undo_last()
            compensated += 1

        self.assertEqual(compensated, 2)
        self.assertFalse(slip.is_in_progress)


if __name__ == '__main__':
    unittest.main()
