"""Work log - record of completed activity work."""

from typing import TYPE_CHECKING

from ascetic_ddd.saga.work_result import WorkResult

if TYPE_CHECKING:
    from ascetic_ddd.saga.activity import Activity


__all__ = (
    'WorkLog',
)


class WorkLog:
    """Record of completed work from an activity.

    Stores the activity type and its result, enabling compensation
    to be performed later if the saga needs to be rolled back.
    """

    def __init__(self, activity: 'Activity', result: WorkResult):
        """Initialize work log.

        Args:
            activity: The activity that performed the work.
            result: The result dictionary from do_work().
        """
        self._activity_type: type['Activity'] = type(activity)
        self._result: WorkResult = result

    @property
    def result(self) -> WorkResult:
        """The result dictionary from the activity's work."""
        return self._result

    @property
    def activity_type(self) -> type['Activity']:
        """The type of activity that performed this work."""
        return self._activity_type
