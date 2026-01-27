"""Reserve flight activity - example activity for travel booking saga."""

import random

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'ReserveFlightActivity',
)


class ReserveFlightActivity(Activity):
    """Activity for reserving a flight.

    This is the highest risk step in a travel booking saga,
    as flights often have strict refund policies.

    Note: In the original example, this activity intentionally fails
    to demonstrate the compensation mechanism.
    """

    _rnd = random.Random(3)

    def do_work(self, work_item: WorkItem) -> WorkLog:
        """Reserve a flight.

        Args:
            work_item: Must contain 'destination' in arguments.

        Returns:
            WorkLog with 'reservationId' in result.

        Raises:
            KeyError: If 'destination' argument is missing (demonstrates failure).
        """
        destination = work_item.arguments["destination"]
        reservation_id = self._rnd.randint(0, 99999)
        return WorkLog(self, WorkResult({"reservationId": reservation_id}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        """Cancel the flight reservation.

        Args:
            work_log: The work log containing the reservation ID.
            routing_slip: The current routing slip.

        Returns:
            True to continue backward compensation.
        """
        reservation_id = work_log.result["reservationId"]
        return True

    @property
    def work_item_queue_address(self) -> str:
        """Queue address for flight reservation requests."""
        return "sb://./flightReservations"

    @property
    def compensation_queue_address(self) -> str:
        """Queue address for flight cancellation requests."""
        return "sb://./flightCancellations"


class FailingReserveFlightActivity(ReserveFlightActivity):
    """Flight activity that always fails - for demonstrating compensation."""

    def do_work(self, work_item: WorkItem) -> WorkLog:
        """Attempt to reserve a flight (always fails).

        This activity intentionally accesses a non-existent key
        to demonstrate the saga's compensation mechanism.

        Raises:
            KeyError: Always, to trigger compensation.
        """
        _ = work_item.arguments["fatzbatz"]  # This throws KeyError
        return super().do_work(work_item)
