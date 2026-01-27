# Saga Pattern

A Python implementation of the Saga pattern using the **Routing Slip** approach, based on [Clemens Vasters' article](https://vasters.com/archive/Sagas.html).

## What is a Saga?

A Saga is a **failure management pattern** for handling long-lived and distributed transactions across systems that cannot use traditional ACID transactions with two-phase commit.

Rather than holding locks across multiple services, a Saga splits work into individual activities whose effects can be **compensated** (reversed) after work has been performed and committed.

## When to Use Sagas

Traditional distributed transactions with locks are impractical when:

- Work spans multiple independent services with different trust boundaries
- Transactions are long-lived and geographically distributed
- Participants cannot be enlisted in a single ACID transaction
- Services are autonomous and may be temporarily unavailable

## How It Works

The pattern operates through a **routing slip** mechanism:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Activity 1 │───>│  Activity 2 │───>│  Activity 3 │
│  (do_work)  │    │  (do_work)  │    │  (do_work)  │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       │                  │                  │ FAILURE!
       │                  │                  ▼
       │                  │           ┌─────────────┐
       │                  │<──────────│ compensate  │
       │                  ▼           └─────────────┘
       │           ┌─────────────┐
       │<──────────│ compensate  │
       ▼           └─────────────┘
┌─────────────┐
│ compensate  │
└─────────────┘
```

1. **Forward Path**: The routing slip progresses through sequential work items
2. **Backward Path**: On failure, the routing slip reverses through completed steps for compensation

### Key Characteristics

- No centralized coordinator
- All work remains local to individual nodes
- The routing slip carries all transaction context
- Decisions occur locally at each step
- Can be serialized and transmitted between distributed systems

## Components

### Activity

Each activity encapsulates two operations:

```python
from ascetic_ddd.saga import Activity, WorkItem, WorkLog, RoutingSlip

class MyActivity(Activity):
    def do_work(self, work_item: WorkItem) -> WorkLog:
        # Perform the business operation
        result = perform_operation(work_item.arguments)
        return WorkLog(self, WorkResult({"id": result.id}))

    def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        # Reverse the operation
        cancel_operation(work_log.result["id"])
        return True  # Continue backward

    @property
    def work_item_queue_address(self) -> str:
        return "sb://./myActivityQueue"

    @property
    def compensation_queue_address(self) -> str:
        return "sb://./myActivityCompensation"
```

### RoutingSlip

The document flowing through the system:

```python
from ascetic_ddd.saga import RoutingSlip, WorkItem, WorkItemArguments

routing_slip = RoutingSlip([
    WorkItem(Activity1, WorkItemArguments({"key": "value1"})),
    WorkItem(Activity2, WorkItemArguments({"key": "value2"})),
    WorkItem(Activity3, WorkItemArguments({"key": "value3"})),
])
```

**Properties:**
- `is_completed`: True if all work items processed
- `is_in_progress`: True if some work completed (can compensate)
- `progress_uri`: Next activity's work queue address
- `compensation_uri`: Last completed activity's compensation address

**Methods:**
- `process_next()`: Execute next work item, returns success/failure
- `undo_last()`: Compensate last completed work

### ActivityHost

Manages message processing for a specific activity type:

```python
from ascetic_ddd.saga import ActivityHost

def send(uri: str, routing_slip: RoutingSlip):
    # Route to appropriate host based on URI
    ...

host = ActivityHost(MyActivity, send)
host.accept_message(uri, routing_slip)
```

## Example: Travel Booking Saga

```python
from ascetic_ddd.saga import RoutingSlip, WorkItem, WorkItemArguments
from ascetic_ddd.saga.examples import (
    ReserveCarActivity,
    ReserveHotelActivity,
    ReserveFlightActivity,
)

# Create routing slip with activities ordered by risk (least risky first)
routing_slip = RoutingSlip([
    WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "Compact"})),
    WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
    WorkItem(ReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
])

# Process the saga
while not routing_slip.is_completed:
    if not routing_slip.process_next():
        # Activity failed - compensate all completed work
        print("Failure! Starting compensation...")
        while routing_slip.is_in_progress:
            routing_slip.undo_last()
        break
else:
    print("Saga completed successfully!")
```

## Risk Ordering Strategy

Activities should be sequenced by success probability (least risky first):

1. **Car reservations** - Highest success rate, easily cancellable
2. **Hotel bookings** - Moderate risk, cancellable until 24 hours before
3. **Airfare** - Highest risk due to refund restrictions

This minimizes the amount of compensation needed when failures occur.

## Compensation Semantics

The `compensate()` method returns a boolean:

- `True`: Compensation succeeded, continue backward path
- `False`: Compensation added new work, resume forward path

This allows for sophisticated recovery strategies where compensation might involve retrying with different parameters.

## Distributed Execution

For distributed systems, use `ActivityHost` with message queues:

```python
from ascetic_ddd.saga import ActivityHost, RoutingSlip

# Each service hosts its own activities
car_host = ActivityHost(ReserveCarActivity, send_message)
hotel_host = ActivityHost(ReserveHotelActivity, send_message)
flight_host = ActivityHost(ReserveFlightActivity, send_message)

hosts = [car_host, hotel_host, flight_host]

def send_message(uri: str, routing_slip: RoutingSlip):
    # In production: serialize and send to message queue
    for host in hosts:
        if host.accept_message(uri, routing_slip):
            break

# Start the saga
send_message(routing_slip.progress_uri, routing_slip)
```

## References

- [Sagas](https://vasters.com/archive/Sagas.html) - Clemens Vasters
- [Sagas (Original Paper)](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) - Garcia-Molina & Salem, 1987
- [Enterprise Integration Patterns: Routing Slip](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html)
- [Source Code](https://gist.github.com/clemensv/3562597)

