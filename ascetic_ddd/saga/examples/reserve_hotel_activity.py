"""Reserve hotel activity - example activity for travel booking saga."""

import random

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'ReserveHotelActivity',
)


class ReserveHotelActivity(Activity):
    """Activity for reserving a hotel room.

    This is a moderate risk step in a travel booking saga,
    as hotels typically allow cancellation until 24 hours before check-in.
    """

    _rnd = random.Random(1)

    def do_work(self, work_item: WorkItem) -> WorkLog:
        """Reserve a hotel room.

        Args:
            work_item: Must contain 'roomType' in arguments.

        Returns:
            WorkLog with 'reservationId' in result.
        """
        room_type = work_item.arguments["roomType"]
        reservation_id = self._rnd.randint(0, 99999)
        return WorkLog(self, WorkResult({"reservationId": reservation_id}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        """Cancel the hotel reservation.

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
        """Queue address for hotel reservation requests."""
        return "sb://./hotelReservations"

    @property
    def compensation_queue_address(self) -> str:
        """Queue address for hotel cancellation requests."""
        return "sb://./hotelCancellations"
