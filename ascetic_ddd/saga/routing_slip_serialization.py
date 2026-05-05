"""Conversion between RoutingSlip and SerializableRoutingSlip.

A RoutingSlip references activity *classes* directly, which are not
JSON-serializable. Conversion to and from the wire format goes through
an ActivityTypeResolver that translates between activity classes and
their canonical names.
"""

from ascetic_ddd.saga.activity_resolver import ActivityTypeResolver
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.serializable_routing_slip import (
    SerializableRoutingSlip,
    SerializableWorkItem,
    SerializableWorkLog,
)
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_log import WorkLog


__all__ = (
    'from_serializable',
    'to_serializable',
)


def to_serializable(
    routing_slip: RoutingSlip,
    resolver: ActivityTypeResolver,
) -> SerializableRoutingSlip:
    """Convert a RoutingSlip into its serializable form.

    Args:
        routing_slip: The routing slip to serialize.
        resolver: Resolver used to translate activity classes into names.

    Returns:
        A SerializableRoutingSlip carrying activity names instead of classes,
        ready to be marshaled to JSON via to_dict().

    Raises:
        KeyError: If any activity class involved is neither registered in
            the resolver nor implements NamedActivity.
    """
    return SerializableRoutingSlip(
        completed_work_logs=[
            SerializableWorkLog(
                activity_type_name=resolver.get_name(log.activity_type),
                result=log.result,
            )
            for log in routing_slip.completed_work_logs
        ],
        next_work_items=[
            SerializableWorkItem(
                activity_type_name=resolver.get_name(item.activity_type),
                arguments=item.arguments,
            )
            for item in routing_slip.pending_work_items
        ],
    )


def from_serializable(
    serializable: SerializableRoutingSlip,
    resolver: ActivityTypeResolver,
) -> RoutingSlip:
    """Reconstruct a RoutingSlip from its serializable form.

    Args:
        serializable: The serializable form (typically obtained by
            unmarshaling JSON via SerializableRoutingSlip.from_dict()).
        resolver: Resolver used to translate activity names back to classes.

    Returns:
        A RoutingSlip in the same state as the original (same completed
        work logs, same pending work items, in the same order).

    Raises:
        KeyError: If any activity name is not registered in the resolver.
    """
    next_items = [
        WorkItem(
            resolver.resolve(item.activity_type_name),
            item.arguments,
        )
        for item in serializable.next_work_items
    ]
    routing_slip = RoutingSlip(next_items)

    # The completed_work_logs property returns the underlying list by
    # reference (matching Go where CompletedWorkLogs() returns the slice
    # header), so appending to it restores the backward-path stack in
    # place without exposing internal state.
    for log in serializable.completed_work_logs:
        activity_type = resolver.resolve(log.activity_type_name)
        activity = activity_type()
        routing_slip.completed_work_logs.append(WorkLog(activity, log.result))

    return routing_slip
