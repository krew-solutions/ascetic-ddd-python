"""Abstract Activity - base class for saga activities."""

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ascetic_ddd.saga.work_item import WorkItem
    from ascetic_ddd.saga.work_log import WorkLog
    from ascetic_ddd.saga.routing_slip import RoutingSlip


__all__ = (
    'Activity',
)


class Activity(metaclass=ABCMeta):
    """Abstract base class for saga activities.

    Each activity encapsulates two operations:
    - do_work(): Performs the actual business operation
    - compensate(): Reverses the operation if the saga fails

    Activities are executed by ActivityHost and their results
    are tracked in the RoutingSlip.
    """

    @abstractmethod
    def do_work(self, work_item: 'WorkItem') -> 'WorkLog':
        """Execute the activity's business logic.

        Args:
            work_item: The work item containing arguments for this activity.

        Returns:
            WorkLog containing the result of the work, or None if failed.
        """
        ...

    @abstractmethod
    def compensate(self, work_log: 'WorkLog', routing_slip: 'RoutingSlip') -> bool:
        """Compensate (undo) the previously completed work.

        Called during the backward path when the saga needs to be rolled back.

        Args:
            work_log: The work log from the original do_work() execution.
            routing_slip: The current routing slip (may be used to add compensating work).

        Returns:
            True if compensation was successful and should continue backward,
            False if compensation added new work and should resume forward path.
        """
        ...

    @property
    @abstractmethod
    def work_item_queue_address(self) -> str:
        """Address of the queue for processing work items (forward path)."""
        ...

    @property
    @abstractmethod
    def compensation_queue_address(self) -> str:
        """Address of the queue for processing compensation (backward path)."""
        ...
