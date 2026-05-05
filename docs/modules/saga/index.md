# Saga Pattern

```{index} Saga, Distributed Transactions, Routing Slip, Compensation, Idempotency
```

A Python implementation of the Saga pattern using the **Routing Slip** approach, based on [Clemens Vasters' article](https://vasters.com/archive/Sagas.html).

## What is a Saga?

A Saga is a **failure management pattern** for handling long-lived and distributed transactions across systems that cannot use traditional ACID transactions with two-phase commit.

Rather than holding locks across multiple services, a Saga splits work into individual activities whose effects can be **compensated** (reversed) after work has been performed and committed.

The {term}`Saga` module provides coordination for distributed transactions using compensating actions.


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
    async def do_work(self, work_item: WorkItem) -> WorkLog:
        # Perform the business operation
        result = await perform_operation(work_item.arguments)
        return WorkLog(self, WorkResult({"id": result.id}))

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        # Reverse the operation
        await cancel_operation(work_log.result["id"])
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
await host.accept_message(uri, routing_slip)
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
    if not await routing_slip.process_next():
        # Activity failed - compensate all completed work
        print("Failure! Starting compensation...")
        while routing_slip.is_in_progress:
            await routing_slip.undo_last()
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


## Idempotency Requirement

**Activities MUST be idempotent.** This is a fundamental requirement for saga reliability.


### Why Idempotency?

In distributed systems with message queues, messages are typically delivered with "at least once" semantics:

1. Worker receives message from queue
2. Worker processes the saga step
3. Worker sends **Ack** (acknowledgment) to queue
4. If worker crashes before Ack, message returns to queue and is redelivered

This means `do_work()` and `compensate()` may be called multiple times for the same logical operation.


### Ensuring Idempotency

```python
class ReserveHotelActivity(Activity):
    async def do_work(self, work_item: WorkItem) -> WorkLog:
        reservation_key = work_item.arguments["idempotency_key"]

        # Check if already processed
        existing = await db.find_reservation(reservation_key)
        if existing:
            return WorkLog(self, WorkResult({"id": existing.id}))

        # Create new reservation with idempotency key
        reservation = await db.create_reservation(
            key=reservation_key,
            room_type=work_item.arguments["roomType"],
        )
        return WorkLog(self, WorkResult({"id": reservation.id}))

    async def compensate(self, work_log: WorkLog, routing_slip: RoutingSlip) -> bool:
        reservation_id = work_log.result["id"]

        # Idempotent cancellation - safe to call multiple times
        await db.cancel_reservation_if_exists(reservation_id)
        return True
```


### ParallelActivity and FallbackActivity

Both `ParallelActivity` and `FallbackActivity` execute branches **in-process** using asyncio:

- **ParallelActivity**: Uses `asyncio.gather()` for concurrent execution
- **FallbackActivity**: Uses sequential loop to try alternatives

If the process crashes during execution:
1. No Ack is sent to the message queue
2. The message is redelivered
3. The entire step (including all branches) is re-executed
4. Idempotent activities ensure correctness on retry

This design avoids the complexity of distributed coordination while maintaining reliability through idempotency.

## Parallel Execution (Fork/Join)

Based on Section 8 of the original Sagas paper, `ParallelActivity` executes multiple RoutingSlips concurrently.
Each branch is a full saga with its own forward/backward paths:

```python
from ascetic_ddd.saga import (
    RoutingSlip, WorkItem, WorkItemArguments, ParallelActivity
)

# T1 → (T2a,T2b || T3) → T4
routing_slip = RoutingSlip([
    WorkItem(ReserveFlightActivity, WorkItemArguments({"destination": "DUS"})),
    WorkItem(ParallelActivity, WorkItemArguments({
        "branches": [
            RoutingSlip([
                WorkItem(ReserveHotelActivity, WorkItemArguments({"room": "Suite"})),
                WorkItem(ConfirmHotelActivity, WorkItemArguments({})),
            ]),
            RoutingSlip([
                WorkItem(ReserveCarActivity, WorkItemArguments({"type": "Compact"})),
            ]),
        ]
    })),
    WorkItem(SendConfirmationActivity, WorkItemArguments({})),
])

# Process the saga
while not routing_slip.is_completed:
    if not await routing_slip.process_next():
        while routing_slip.is_in_progress:
            await routing_slip.undo_last()
        break
```

**Behavior:**
- Each branch is a full RoutingSlip (multi-step saga)
- All branches execute concurrently
- **Fail-fast**: On first failure, all branches are compensated
- **Compensation**: All branches compensated in parallel (reverse order within each)


## Recovery Blocks (Fallback)

Based on Section 6 of the original paper, `FallbackActivity` tries alternative RoutingSlips until one succeeds:

```python
from ascetic_ddd.saga import (
    RoutingSlip, WorkItem, WorkItemArguments, FallbackActivity
)

# Try primary payment flow, if fails try backup flow
routing_slip = RoutingSlip([
    WorkItem(PrepareOrderActivity, WorkItemArguments({})),
    WorkItem(FallbackActivity, WorkItemArguments({
        "alternatives": [
            RoutingSlip([
                WorkItem(PrimaryPaymentActivity, WorkItemArguments({"amount": 100})),
                WorkItem(ConfirmPaymentActivity, WorkItemArguments({})),
            ]),
            RoutingSlip([
                WorkItem(BackupPaymentActivity, WorkItemArguments({"amount": 100})),
            ]),
        ]
    })),
    WorkItem(ShipOrderActivity, WorkItemArguments({})),
])
```

**Behavior:**
- Each alternative is a full RoutingSlip (multi-step saga)
- Tries each alternative in order
- If alternative fails, it compensates itself before trying next
- Only the successful alternative needs compensation later
- If all alternatives fail, returns `None` (saga can compensate previous steps)


## Combining Parallel and Fallback

Activities can be nested for complex scenarios:

```python
# Flight → (Hotel saga || Car with fallback) → Confirmation
routing_slip = RoutingSlip([
    WorkItem(ReserveFlightActivity, WorkItemArguments({"dest": "DUS"})),
    WorkItem(ParallelActivity, WorkItemArguments({
        "branches": [
            # Branch 1: Hotel saga
            RoutingSlip([
                WorkItem(ReserveHotelActivity, WorkItemArguments({"room": "Suite"})),
                WorkItem(ConfirmHotelActivity, WorkItemArguments({})),
            ]),
            # Branch 2: Car with fallback providers
            RoutingSlip([
                WorkItem(FallbackActivity, WorkItemArguments({
                    "alternatives": [
                        RoutingSlip([WorkItem(ReserveHertzActivity, WorkItemArguments({}))]),
                        RoutingSlip([WorkItem(ReserveAvisActivity, WorkItemArguments({}))]),
                    ]
                })),
            ]),
        ]
    })),
    WorkItem(SendConfirmationActivity, WorkItemArguments({})),
])
```


## Distributed Execution

For distributed systems, use `ActivityHost` with message queues:

```python
from ascetic_ddd.saga import ActivityHost, RoutingSlip

# Each service hosts its own activities
car_host = ActivityHost(ReserveCarActivity, send_message)
hotel_host = ActivityHost(ReserveHotelActivity, send_message)
flight_host = ActivityHost(ReserveFlightActivity, send_message)

hosts = [car_host, hotel_host, flight_host]

async def send_message(uri: str, routing_slip: RoutingSlip):
    # In production: serialize and send to message queue
    for host in hosts:
        if await host.accept_message(uri, routing_slip):
            break

# Start the saga
await send_message(routing_slip.progress_uri, routing_slip)
```

## RoutingSlip Serialization

To run a saga across services, the `RoutingSlip` must travel with the message that
hands work off to the next participant. `RoutingSlip` references activity *classes*
directly, which are not JSON-serializable, so an additional indirection is needed:
each side of the wire keeps a registry that translates between activity classes
and their canonical names (strings).

### Overview

The serialization mechanism follows an **interface-based** approach with dependency
injection. There is no global registry: each `ActivityTypeResolver` instance is
independent, which prevents state pollution across tests and across services that
should not know about each other's activities.


### Components

#### 1. ActivityTypeResolver

Abstract base class providing bidirectional mapping between activity type names
and activity classes:

```python
from abc import ABCMeta, abstractmethod

class ActivityTypeResolver(metaclass=ABCMeta):
    @abstractmethod
    def resolve(self, type_name: str) -> type[Activity]: ...

    @abstractmethod
    def get_name(self, activity_type: type[Activity]) -> str: ...
```


#### 2. MapBasedResolver

Default in-memory implementation backed by a pair of dicts (`name → type`,
`type → name`):

```python
from ascetic_ddd.saga import MapBasedResolver

resolver = MapBasedResolver()
resolver.register("ReserveCarActivity", ReserveCarActivity)
resolver.register("ReserveHotelActivity", ReserveHotelActivity)
```


#### 3. NamedActivity Protocol (Optional)

Activities can optionally implement the `NamedActivity` protocol to expose their
canonical name:

```python
from ascetic_ddd.saga import Activity

class ReserveCarActivity(Activity):
    def type_name(self) -> str:
        return "ReserveCarActivity"
    # ... do_work, compensate, work_item_queue_address, compensation_queue_address
```

**Benefits:**

- Enables fallback name resolution in `get_name()` even without explicit registration
- Deserialization still requires registration (no name → type mapping otherwise)
- Makes activity type names self-documenting on the class itself

`NamedActivity` is a `runtime_checkable` `Protocol`, so any class that defines a
`type_name()` method satisfies it structurally — no inheritance required.


### Basic Usage

#### Step 1: Create a Resolver and Register Activities

```python
from ascetic_ddd.saga import MapBasedResolver

resolver = MapBasedResolver()
resolver.register("ReserveCarActivity", ReserveCarActivity)
resolver.register("ReserveHotelActivity", ReserveHotelActivity)
resolver.register("ReserveFlightActivity", ReserveFlightActivity)
```

#### Step 2: Serialize RoutingSlip

```python
import json

from ascetic_ddd.saga import (
    RoutingSlip, WorkItem, WorkItemArguments, to_serializable,
)

routing_slip = RoutingSlip([
    WorkItem(ReserveCarActivity, WorkItemArguments({"vehicleType": "SUV"})),
    WorkItem(ReserveHotelActivity, WorkItemArguments({"roomType": "Suite"})),
])

await routing_slip.process_next()

serializable = to_serializable(routing_slip, resolver)
wire = json.dumps(serializable.to_dict())

await bus.publish(session, "saga/routing-slip", wire)
```

#### Step 3: Deserialize RoutingSlip

```python
import json

from ascetic_ddd.saga import (
    SerializableRoutingSlip, from_serializable,
)

wire = await bus.receive("saga/routing-slip")

serializable = SerializableRoutingSlip.from_dict(json.loads(wire))
routing_slip = from_serializable(serializable, resolver)

await routing_slip.process_next()
```


### Wire Format

`SerializableRoutingSlip.to_dict()` produces JSON-compatible dicts with **camelCase**
keys, identical to the format produced by the Go implementation. This allows mixed
Python/Go services to participate in the same distributed saga.

```json
{
  "completedWorkLogs": [
    {
      "activityTypeName": "ReserveCarActivity",
      "result": {
        "reservationId": 12345
      }
    }
  ],
  "nextWorkItems": [
    {
      "activityTypeName": "ReserveHotelActivity",
      "arguments": {
        "roomType": "Suite",
        "checkInDate": "2024-01-15"
      }
    },
    {
      "activityTypeName": "ReserveFlightActivity",
      "arguments": {
        "destination": "LAX",
        "flightDate": "2024-01-15"
      }
    }
  ]
}
```


### Advanced Patterns

#### Multiple Resolvers for Different Services

Service-specific resolvers limit which activities each service can deserialize —
useful as a defense-in-depth measure:

```python
# Orchestrator knows every activity
orchestrator_resolver = MapBasedResolver()
orchestrator_resolver.register("ReserveCarActivity", ReserveCarActivity)
orchestrator_resolver.register("ReserveHotelActivity", ReserveHotelActivity)
orchestrator_resolver.register("ReserveFlightActivity", ReserveFlightActivity)

# Car service only knows car activities
car_service_resolver = MapBasedResolver()
car_service_resolver.register("ReserveCarActivity", ReserveCarActivity)

# Hotel service only knows hotel activities
hotel_service_resolver = MapBasedResolver()
hotel_service_resolver.register("ReserveHotelActivity", ReserveHotelActivity)
```

#### Compensation Serialization

The same serialization works for the backward path:

```python
serializable = to_serializable(routing_slip, resolver)
wire = json.dumps(serializable.to_dict())

await bus.publish(session, routing_slip.compensation_uri, wire)

# On the compensation service:
serializable = SerializableRoutingSlip.from_dict(json.loads(wire))
routing_slip = from_serializable(serializable, resolver)

while routing_slip.is_in_progress:
    await routing_slip.undo_last()
```

#### Testing with Isolated Resolvers

Each test creates its own resolver — no global state to clean up:

```python
class MySagaTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.resolver = MapBasedResolver()
        self.resolver.register("TestActivity", TestActivity)
```


### Design Rationale

#### Why Not a Global Registry?

The interface-based approach was preferred over a global module-level registry
because:

1. **No global state** — each resolver is independent, preventing pollution between
   tests and between services.
2. **Better testability** — tests construct isolated resolvers with no cleanup.
3. **Explicit dependencies** — resolvers are passed explicitly, making the
   dependency on the registry visible at the call site.
4. **Service isolation** — different services can have different resolver
   configurations without shared mutable state.
5. **Thread safety** — no need for locks on a global mutable map (though
   `MapBasedResolver` itself is also not thread-safe; register everything at
   startup).

#### Trade-offs

**Pros:** clean dependency injection, isolation, no global state.

**Cons:** slightly more verbose (the resolver must be passed explicitly), and a
resolver is required on both ends of the wire.


### Error Handling

#### Unregistered Activity (Deserialization)

```python
from_serializable(serializable, resolver)
# KeyError: activity type not registered: UnknownActivity
```

**Solution:** register all expected activities before deserializing.

#### Unregistered Activity Without NamedActivity (Serialization)

```python
to_serializable(routing_slip, resolver)
# KeyError: activity type not registered: AnonymousActivity
```

**Solution:** register the activity, or implement `type_name()` on the class.


### Best Practices

1. **Register activities at module-load time:**

   ```python
   def make_resolver() -> MapBasedResolver:
       resolver = MapBasedResolver()
       resolver.register("Activity1", Activity1)
       resolver.register("Activity2", Activity2)
       return resolver
   ```

2. **Implement `type_name()` on every activity** so serialization keeps working
   even if a registration is forgotten:

   ```python
   class MyActivity(Activity):
       def type_name(self) -> str:
           return "MyActivity"
   ```

3. **Use descriptive names** rather than abbreviations — they end up on the wire
   and in logs:

   ```python
   resolver.register("ReserveCarActivity", ReserveCarActivity)
   # not: resolver.register("car", ReserveCarActivity)
   ```

4. **Share resolver configuration** through a single factory function so every
   service spins up an identically configured resolver.

5. **Test round-trip serialization** to catch missing registrations and
   non-JSON-safe arguments early:

   ```python
   async def test_round_trip(self):
       original = make_routing_slip()
       wire = json.dumps(to_serializable(original, resolver).to_dict())
       restored = from_serializable(
           SerializableRoutingSlip.from_dict(json.loads(wire)),
           resolver,
       )
       self.assertEqual(
           len(restored.pending_work_items),
           len(original.pending_work_items),
       )
   ```


## References

- [Sagas](https://vasters.com/archive/Sagas.html) - Clemens Vasters
- [Sagas (Original Paper)](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) - Garcia-Molina & Salem, 1987
- [Enterprise Integration Patterns: Routing Slip](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html)
- [Source Code](https://gist.github.com/clemensv/3562597)

