"""Routing slip - the document that flows through the saga."""

from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ascetic_ddd.saga.activity import Activity
    from ascetic_ddd.saga.work_item import WorkItem
    from ascetic_ddd.saga.work_log import WorkLog


__all__ = (
    'RoutingSlip',
)


class RoutingSlip:
    """The routing slip that flows through the saga.

    Contains:
    - Queue of pending work items (forward path)
    - Stack of completed work logs (backward path)

    The routing slip carries all transaction context and can be
    serialized for transmission between distributed systems.
    """

    def __init__(self, work_items: list['WorkItem'] | None = None):
        """Initialize routing slip.

        Args:
            work_items: Optional list of work items to process.
        """
        self._completed_work_logs: list['WorkLog'] = []
        self._next_work_items: deque['WorkItem'] = deque()

        if work_items:
            for work_item in work_items:
                self._next_work_items.append(work_item)

    @property
    def is_completed(self) -> bool:
        """True if all work items have been processed."""
        return len(self._next_work_items) == 0

    @property
    def is_in_progress(self) -> bool:
        """True if some work has been completed (can be compensated)."""
        return len(self._completed_work_logs) > 0

    def process_next(self) -> bool:
        """Process the next work item in the queue.

        Returns:
            True if the work was successful, False otherwise.

        Raises:
            InvalidOperationError: If there are no more work items.
        """
        if self.is_completed:
            raise InvalidOperationError("No more work items to process")

        current_item = self._next_work_items.popleft()
        activity: 'Activity' = current_item.activity_type()

        try:
            result = activity.do_work(current_item)
            if result is not None:
                self._completed_work_logs.append(result)
                return True
        except Exception:
            pass

        return False

    @property
    def progress_uri(self) -> str | None:
        """Address of the next activity's work queue, or None if completed."""
        if self.is_completed:
            return None

        activity: 'Activity' = self._next_work_items[0].activity_type()
        return activity.work_item_queue_address

    @property
    def compensation_uri(self) -> str | None:
        """Address of the last completed activity's compensation queue."""
        if not self.is_in_progress:
            return None

        activity: 'Activity' = self._completed_work_logs[-1].activity_type()
        return activity.compensation_queue_address

    def undo_last(self) -> bool:
        """Undo the last completed work item.

        Returns:
            True if compensation succeeded and should continue backward,
            False if compensation added new work and should resume forward.

        Raises:
            InvalidOperationError: If there is no work to undo.
        """
        if not self.is_in_progress:
            raise InvalidOperationError("No work to undo")

        current_item = self._completed_work_logs.pop()
        activity: 'Activity' = current_item.activity_type()

        try:
            return activity.compensate(current_item, self)
        except Exception:
            raise

    @property
    def completed_work_logs(self) -> list['WorkLog']:
        """List of completed work logs (for inspection/testing)."""
        return self._completed_work_logs

    @property
    def pending_work_items(self) -> deque['WorkItem']:
        """Queue of pending work items (for inspection/testing)."""
        return self._next_work_items


class InvalidOperationError(Exception):
    """Raised when an operation is invalid for the current state."""
    pass
