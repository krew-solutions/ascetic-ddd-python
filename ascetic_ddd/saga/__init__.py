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

    # Process the saga
    while not routing_slip.is_completed:
        if not routing_slip.process_next():
            # Compensation needed
            while routing_slip.is_in_progress:
                routing_slip.undo_last()
            break

See Also:
    https://vasters.com/archive/Sagas.html - Original article by Clemens Vasters
"""

from ascetic_ddd.saga.activity import Activity
from ascetic_ddd.saga.activity_host import ActivityHost
from ascetic_ddd.saga.routing_slip import InvalidOperationError, RoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_log import WorkLog
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'Activity',
    'ActivityHost',
    'InvalidOperationError',
    'RoutingSlip',
    'WorkItem',
    'WorkItemArguments',
    'WorkLog',
    'WorkResult',
)
