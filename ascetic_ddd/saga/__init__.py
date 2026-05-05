"""Saga pattern implementation using routing slip approach.

This module implements the Saga pattern for managing distributed transactions
without using traditional two-phase commit. Instead of holding locks across
services, a Saga splits work into individual activities whose effects can be
compensated (reversed) if subsequent steps fail.

Key Components:
- Activity: Base class for saga activities (do_work + compensate)
- WorkItem: Unit of work with arguments for an activity
- WorkLog: Record of completed work for compensation
- RoutingSlip: The document flowing through the saga
- ActivityHost: Processes messages for a specific activity type

Example:
    from ascetic_ddd.saga import (
        RoutingSlip, WorkItem, WorkItemArguments, ActivityHost
    )
    from ascetic_ddd.saga.examples import (
        ReserveCarActivity, ReserveHotelActivity, ReserveFlightActivity
    )

    # Create a routing slip with work items
    routing_slip = RoutingSlip([
        WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
        WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
        WorkItem(ReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
    ])

    # Process the saga (async)
    while not routing_slip.is_completed:
        if not await routing_slip.process_next():
            # Compensation needed
            while routing_slip.is_in_progress:
                await routing_slip.undo_last()
            break

See Also:
    https://vasters.com/archive/Sagas.html - Original article by Clemens Vasters
"""

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.activity_host import ActivityHost
from ascetic_ddd.saga.activity_resolver import (
    ActivityTypeResolver,
    MapBasedResolver,
    NamedActivity,
)
from ascetic_ddd.saga.fallback_activity import FallbackActivity
from ascetic_ddd.saga.parallel_activity import ParallelActivity
from ascetic_ddd.saga.routing_slip import InvalidOperationError, RoutingSlip
from ascetic_ddd.saga.routing_slip_serialization import (
    from_serializable,
    to_serializable,
)
from ascetic_ddd.saga.serializable_routing_slip import (
    SerializableRoutingSlip,
    SerializableWorkItem,
    SerializableWorkLog,
)
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'Activity',
    'ActivityHost',
    'ActivityTypeResolver',
    'FallbackActivity',
    'InvalidOperationError',
    'MapBasedResolver',
    'NamedActivity',
    'ParallelActivity',
    'RoutingSlip',
    'SerializableRoutingSlip',
    'SerializableWorkItem',
    'SerializableWorkLog',
    'WorkItem',
    'WorkItemArguments',
    'WorkLog',
    'WorkResult',
    'from_serializable',
    'to_serializable',
)
