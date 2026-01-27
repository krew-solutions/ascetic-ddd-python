"""Work item - unit of work to be processed by an activity."""

from typing import TYPE_CHECKING, Generic, TypeVar

from ascetic_ddd.saga.work_item_arguments import WorkItemArguments

if TYPE_CHECKING:
    from ascetic_ddd.saga.activity import Activity
    from ascetic_ddd.saga.routing_slip import RoutingSlip


__all__ = (
    'WorkItem',
)


T = TypeVar('T', bound='Activity')


class WorkItem(Generic[T]):
    """A unit of work to be processed by a specific activity type.

    Contains the arguments needed by the activity and a reference
    to the routing slip it belongs to.
    """

    def __init__(self, activity_type: type[T], arguments: WorkItemArguments):
        """Initialize work item.

        Args:
            activity_type: The type of activity that will process this work item.
            arguments: Dictionary of arguments for the activity.
        """
        self._activity_type: type[T] = activity_type
        self._arguments: WorkItemArguments = arguments
        self._routing_slip: 'RoutingSlip | None' = None

    @property
    def activity_type(self) -> type[T]:
        """The type of activity that will process this work item."""
        return self._activity_type

    @property
    def arguments(self) -> WorkItemArguments:
        """The arguments for the activity."""
        return self._arguments

    @property
    def routing_slip(self) -> 'RoutingSlip | None':
        """The routing slip this work item belongs to."""
        return self._routing_slip

    @routing_slip.setter
    def routing_slip(self, value: 'RoutingSlip') -> None:
        """Set the routing slip for this work item."""
        self._routing_slip = value
