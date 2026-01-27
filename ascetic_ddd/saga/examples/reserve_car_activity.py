"""Reserve car activity - example activity for travel booking saga."""

import random

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'ReserveCarActivity',
)


class ReserveCarActivity(Activity):
    """Activity for reserving a rental car.

    This is typically the least risky step in a travel booking saga,
    as car reservations are usually easy to cancel.
    """

    _rnd = random.Random(2)

    def do_work(self, work_item: WorkItem) -> WorkLog:
        """Reserve a car.

        Args:
            work_item: Must contain 'vehicleType' in arguments.

        Returns:
            WorkLog with 'reservationId' in result.
        """
        vehicle_type = work_item.arguments["vehicleType"]
        reservation_id = self._rnd.randint(0, 99999)
        return WorkLog(self, WorkResult({"reservationId": reservation_id}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        """Cancel the car reservation.

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
        """Queue address for car reservation requests."""
        return "sb://./carReservations"

    @property
    def compensation_queue_address(self) -> str:
        """Queue address for car cancellation requests."""
        return "sb://./carCancellations"
