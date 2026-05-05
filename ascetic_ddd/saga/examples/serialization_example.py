"""End-to-end demonstration of RoutingSlip serialization.

Shows how a saga can be paused on one service, transmitted over a message
bus as JSON, and resumed on another service. Two scenarios are included:

* run_travel_booking_with_serialization: forward path, one round-trip after
  the first activity, then continue processing on the receiving side.
* run_compensation_with_serialization: forward path until a deliberate
  failure, then a round-trip to a "compensation service" that runs the
  backward path.

Run as a script:

    python -m ascetic_ddd.saga.examples.serialization_example
"""

import asyncio
import json

from ascetic_ddd.saga.activity_resolver import MapBasedResolver
from ascetic_ddd.saga.examples.reserve_car_activity import ReserveCarActivity
from ascetic_ddd.saga.examples.reserve_flight_activity import (
    FailingReserveFlightActivity,
    ReserveFlightActivity,
)
from ascetic_ddd.saga.examples.reserve_hotel_activity import ReserveHotelActivity
from ascetic_ddd.saga.routing_slip import RoutingSlip
from ascetic_ddd.saga.routing_slip_serialization import (
    from_serializable,
    to_serializable,
)
from ascetic_ddd.saga.serializable_routing_slip import SerializableRoutingSlip
from ascetic_ddd.saga.work_item import WorkItem
from ascetic_ddd.saga.work_item_arguments import WorkItemArguments


__all__ = (
    'make_orchestrator_resolver',
    'run_compensation_with_serialization',
    'run_travel_booking_with_serialization',
    'transmit',
)


def make_orchestrator_resolver() -> MapBasedResolver:
    """Resolver that knows every example activity.

    A real distributed deployment would typically use narrower per-service
    resolvers (see "Multiple Resolvers for Different Services" in the docs);
    a single orchestrator-wide resolver is used here to keep the example
    self-contained.
    """
    resolver = MapBasedResolver()
    resolver.register("ReserveCarActivity", ReserveCarActivity)
    resolver.register("ReserveHotelActivity", ReserveHotelActivity)
    resolver.register("ReserveFlightActivity", ReserveFlightActivity)
    # FailingReserveFlightActivity reuses its parent's canonical name on
    # purpose: on the wire there is no difference between "succeed" and
    # "fail" implementations -- the receiving service decides which class
    # to bind by what is registered.
    resolver.register("FailingReserveFlightActivity", FailingReserveFlightActivity)
    return resolver


def transmit(routing_slip: RoutingSlip, resolver: MapBasedResolver) -> RoutingSlip:
    """Round-trip a routing slip through JSON, simulating a message bus.

    Args:
        routing_slip: The slip to serialize on the sending side.
        resolver: Resolver shared (in this example) by both ends.

    Returns:
        A new RoutingSlip reconstructed on the receiving side.
    """
    wire = json.dumps(to_serializable(routing_slip, resolver).to_dict())
    print("---- on the wire ----")
    print(wire)
    print("---------------------")
    return from_serializable(
        SerializableRoutingSlip.from_dict(json.loads(wire)),
        resolver,
    )


async def run_travel_booking_with_serialization() -> RoutingSlip:
    """Forward-path scenario with one mid-saga handoff.

    Sequence:
        1. Orchestrator builds the routing slip and processes the car step.
        2. Slip is serialized, "shipped" via JSON, and reconstructed on a
           downstream service.
        3. Downstream service finishes hotel + flight.

    Returns:
        The routing slip in its final, fully-completed state.
    """
    resolver = make_orchestrator_resolver()

    routing_slip = RoutingSlip([
        WorkItem(ReserveCarActivity, WorkItemArguments({
            "vehicleType": "SUV",
            "pickupDate": "2024-01-15",
        })),
        WorkItem(ReserveHotelActivity, WorkItemArguments({
            "roomType": "Suite",
            "checkInDate": "2024-01-15",
        })),
        WorkItem(ReserveFlightActivity, WorkItemArguments({
            "destination": "LAX",
            "flightDate": "2024-01-15",
        })),
    ])

    print("\n=== Travel booking saga: process car on orchestrator ===")
    await routing_slip.process_next()

    print("\n=== Hand off to downstream service ===")
    routing_slip = transmit(routing_slip, resolver)

    print("\n=== Resume on downstream service: hotel, then flight ===")
    while not routing_slip.is_completed:
        await routing_slip.process_next()

    print(
        "Done. completed=%d, in_progress=%s"
        % (len(routing_slip.completed_work_logs), routing_slip.is_in_progress)
    )
    return routing_slip


async def run_compensation_with_serialization() -> RoutingSlip:
    """Compensation-path scenario with a handoff to a compensation service.

    Sequence:
        1. Saga runs forward until FailingReserveFlightActivity fails.
        2. The orchestrator serializes the slip and "ships" it to the
           compensation service.
        3. The compensation service runs the backward path to completion.

    Returns:
        The routing slip after compensation -- no completed logs left.
    """
    resolver = make_orchestrator_resolver()

    routing_slip = RoutingSlip([
        WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "SUV"})),
        WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
        WorkItem(FailingReserveFlightActivity, WorkItemArguments({"destination": "LAX"})),
    ])

    print("\n=== Compensation saga: run forward path until failure ===")
    while not routing_slip.is_completed:
        if not await routing_slip.process_next():
            print("Forward step failed -- need to compensate")
            break

    print("\n=== Hand off to compensation service ===")
    routing_slip = transmit(routing_slip, resolver)

    print("\n=== Run backward path on compensation service ===")
    while routing_slip.is_in_progress:
        await routing_slip.undo_last()

    print(
        "Done. completed=%d, in_progress=%s"
        % (len(routing_slip.completed_work_logs), routing_slip.is_in_progress)
    )
    return routing_slip


async def _main() -> None:
    await run_travel_booking_with_serialization()
    await run_compensation_with_serialization()


if __name__ == "__main__":
    asyncio.run(_main())
