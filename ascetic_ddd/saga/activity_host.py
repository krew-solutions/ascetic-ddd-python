"""Activity host - processes messages for a specific activity type."""

from typing import Callable, Generic, TypeVar

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.routing_slip import RoutingSlip


__all__ = (
    'ActivityHost',
)


T = TypeVar('T', bound=Activity)

SendCallback = Callable[[str, RoutingSlip], None]


class ActivityHost(Generic[T]):
    """Host for processing messages for a specific activity type.

    Manages local execution by:
    - Processing forward messages to execute do_work()
    - Processing backward messages to invoke compensate()
    - Routing results to appropriate next addresses
    """

    def __init__(self, activity_type: type[T], send: SendCallback):
        """Initialize activity host.

        Args:
            activity_type: The type of activity this host manages.
            send: Callback function to send routing slip to next address.
        """
        self._activity_type: type[T] = activity_type
        self._send: SendCallback = send

    def process_forward_message(self, routing_slip: RoutingSlip) -> None:
        """Process a forward (do_work) message.

        If work succeeds, sends to next activity's work queue.
        If work fails, sends to compensation queue for rollback.

        Args:
            routing_slip: The routing slip to process.
        """
        if not routing_slip.is_completed:
            if routing_slip.process_next():
                # Success - continue forward
                if routing_slip.progress_uri:
                    self._send(routing_slip.progress_uri, routing_slip)
            else:
                # Failure - start compensation
                if routing_slip.compensation_uri:
                    self._send(routing_slip.compensation_uri, routing_slip)

    def process_backward_message(self, routing_slip: RoutingSlip) -> None:
        """Process a backward (compensate) message.

        If compensation succeeds, continues backward to previous activity.
        If compensation returns False (added new work), resumes forward.

        Args:
            routing_slip: The routing slip to process.
        """
        if routing_slip.is_in_progress:
            if routing_slip.undo_last():
                # Continue backward
                if routing_slip.compensation_uri:
                    self._send(routing_slip.compensation_uri, routing_slip)
            else:
                # Resume forward (compensation added new work)
                if routing_slip.progress_uri:
                    self._send(routing_slip.progress_uri, routing_slip)

    def accept_message(self, uri: str, routing_slip: RoutingSlip) -> bool:
        """Accept and process a message if it matches this host's queues.

        Args:
            uri: The target URI of the message.
            routing_slip: The routing slip to process.

        Returns:
            True if message was accepted and processed, False otherwise.
        """
        activity: Activity = self._activity_type()

        if activity.compensation_queue_address == uri:
            self.process_backward_message(routing_slip)
            return True

        if activity.work_item_queue_address == uri:
            self.process_forward_message(routing_slip)
            return True

        return False
