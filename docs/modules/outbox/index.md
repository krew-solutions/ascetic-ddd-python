# Transactional Outbox

```{index} Outbox, Transactional Outbox, Dual-Write, At-Least-Once Delivery
```

PostgreSQL implementation of the Transactional Outbox pattern for reliable message publishing.


## The Problem: Dual-Write

When a business operation needs to both update the database and publish messages to external systems (message brokers, other services), you cannot guarantee atomicity with traditional approaches:

```python
# DANGEROUS: Not atomic!
await repository.save(order)      # 1. Save to database
await broker.publish(event)       # 2. Publish to message broker
                                  # If crash happens here, message is lost
```

If the application crashes between steps 1 and 2, the database is updated but the message is never published.


## The Solution: Outbox Pattern

Instead of publishing directly, save the message to an `outbox` table within the **same database transaction** as the business state change:

```python
async with session.atomic():
    await repository.save(order)
    await outbox.publish(session, OutboxMessage(
        uri="kafka://orders",
        payload=json.dumps({"type": "OrderCreated", "order_id": str(order.id), "amount": order.amount}).encode(),
        metadata={"message_id": str(uuid4())},
    ))
# Both are committed atomically - or neither
```

A separate dispatcher process reads unpublished messages and sends them to external systems.


## Key Concepts


### 1. Transactional Guarantee

Messages are saved to the Outbox table within the **same database transaction** as business state changes. This ensures that either both the state change AND the message are persisted, or neither is.


### 2. At-Least-Once Delivery

The dispatcher reads unpublished messages and sends them to external systems. If the dispatcher crashes after publishing but before marking as processed, the message will be resent. **Consumers MUST be idempotent**.


### 3. Ordering with transaction_id (xid8)

PostgreSQL's `SERIAL`/`BIGSERIAL` doesn't guarantee ordering in concurrent transactions:

```
TX1: gets position=1, takes 5 seconds
TX2: gets position=2, commits immediately
```

A consumer reading `position > 0` would see message 2 but miss message 1 (still in-flight).

**Solution**: Use `transaction_id` (PostgreSQL `xid8` type) + `position` for ordering:

- `transaction_id` = `pg_current_xact_id()` captures the transaction ID
- Read only messages where `transaction_id < pg_snapshot_xmin(pg_current_snapshot())` (only from COMMITTED transactions visible to all)
- Order by `(transaction_id, position)` for deterministic ordering


### 4. Consumer Groups

Multiple consumers can independently track their position in the outbox. Each consumer group maintains its own `(last_processed_transaction_id, offset_acked)`.


### 5. Visibility Rule

`transaction_id < pg_snapshot_xmin(pg_current_snapshot())` ensures we only read messages from transactions that are fully committed and visible to all sessions.


### 6. URI-Based Filtering and Partitioning

URI serves two purposes:
- **Routing**: Determines where messages are sent (topic, exchange, queue)
- **Partitioning**: Determines which worker processes the message

```python
# URI without partition key — all messages go to one worker
await outbox.publish(session, OutboxMessage(uri="kafka://orders", ...))

# URI with partition key — distributed across workers by hash
await outbox.publish(session, OutboxMessage(uri="kafka://orders/order-123", ...))
await outbox.publish(session, OutboxMessage(uri="kafka://orders/order-456", ...))
```

Dispatch with prefix matching:

```python
# Matches both "kafka://orders" and "kafka://orders/*"
await outbox.dispatch(handler, consumer_group="broker", uri="kafka://orders")
```

Each `(consumer_group, uri)` pair tracks its position independently.


### 7. Partitioning

To ensure message ordering within a partition key, each process/coroutine must process only its assigned partitions:

```python
# Process 0 of 3: hash(uri) % 3 == 0, offset tracked as "broker:0"
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=0, num_processes=3)

# Process 1 of 3: hash(uri) % 3 == 1, offset tracked as "broker:1"
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=1, num_processes=3)

# Process 2 of 3: hash(uri) % 3 == 2, offset tracked as "broker:2"
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=2, num_processes=3)
```

**Why this matters**: If multiple processes write to the same Kafka partition, message order is not guaranteed. By partitioning at the Outbox level, each process writes only to its assigned partitions.

**Offset isolation**: When using multiple workers (via `num_processes * concurrency > 1` in `run()` or `num_workers > 1` in `dispatch()`), each worker automatically gets its own offset tracking. The `consumer_group` is extended with effective worker ID: `"broker"` becomes `"broker:0"`, `"broker:1"`, etc.

**Partition distribution**:

| URI | hash % 3 | Process |
|-----|----------|---------|
| `kafka://orders` | 1 | Process 1 (all messages without partition key) |
| `kafka://orders/order-123` | 0 | Process 0 |
| `kafka://orders/order-456` | 2 | Process 2 |


## Installation

```python
from ascetic_ddd.outbox import Outbox, OutboxMessage
```

## Usage


### Setup

```python
from ascetic_ddd.outbox import Outbox

outbox = Outbox(
    session_pool=pool,
    outbox_table='outbox',         # default
    offsets_table='outbox_offsets', # default
    batch_size=100                  # default
)

# Create tables
async with session_pool.session() as session:
    async with session.atomic():
        await outbox.setup(session)
```

### Publishing Messages

```python
import json
from uuid import uuid4

from ascetic_ddd.outbox import OutboxMessage

async with session.atomic():
    # Business logic
    await repository.save(order)

    # Publish to outbox (same transaction)
    await outbox.publish(session, OutboxMessage(
        uri="kafka://orders",
        payload=json.dumps({
            "type": "OrderCreated",
            "order_id": str(order.id),
            "customer_id": str(order.customer_id),
            "amount": order.amount,
        }).encode(),
        metadata={
            "message_id": str(uuid4()),  # Required for idempotency
            "correlation_id": correlation_id,
            "causation_id": causation_id,
        },
    ))
```

### URI Examples

| URI | Transport | Description |
|-----|-----------|-------------|
| `kafka://orders` | Kafka | Topic "orders" (no partition key) |
| `kafka://orders/order-123` | Kafka | Topic "orders", partition key "order-123" |
| `amqp://exchange/routing.key` | RabbitMQ | Exchange with routing key |
| `sb://./queue-name` | Azure Service Bus | Queue |
| `sqs://queue-name` | AWS SQS | Queue |

The part after the topic/queue name serves as a partition key for worker distribution. All messages with the same full URI go to the same worker, preserving order.

### Dispatching Messages


#### Option 1: dispatch() - Single Batch

```python
async def send_to_broker(message: OutboxMessage) -> None:
    await broker.publish(
        topic=message.uri,
        payload=message.payload,
        headers=message.metadata,
    )

# Process one batch (all URIs)
has_messages = await outbox.dispatch(send_to_broker, consumer_group="broker")

# Process one batch (specific URI)
has_messages = await outbox.dispatch(send_to_broker, consumer_group="broker", uri="kafka://orders")
```

#### Option 2: run() - Continuous Loop

```python
stop_event = asyncio.Event()

# Single coroutine, single process (default)
await outbox.run(
    subscriber=send_to_broker,
    consumer_group="broker",
    uri="kafka://orders",
    poll_interval=1.0,
    stop_event=stop_event,
)

# Multiple coroutines in single process (4 partitions)
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", concurrency=4)

# Multiple processes (run in separate processes, 3 partitions total)
# Process 0:
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=0, num_processes=3)
# Process 1:
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=1, num_processes=3)
# Process 2:
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=2, num_processes=3)

# Multiple processes with multiple coroutines (2 processes × 2 coroutines = 4 partitions)
# Process 0: handles partitions 0, 1
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=0, num_processes=2, concurrency=2)
# Process 1: handles partitions 2, 3
await outbox.run(subscriber, consumer_group="broker", uri="kafka://orders", process_id=1, num_processes=2, concurrency=2)
```

Each coroutine processes its own partitions:
```
effective_id = process_id * concurrency + local_id
effective_total = num_processes * concurrency
```

#### Option 3: Async Iterator

```python
async for message in outbox:
    await send_to_broker(message)
    # Position is updated after yield returns
```

### Position Management

```python
# Get current position (all URIs)
tx_id, offset = await outbox.get_position(session, "broker")

# Get current position (specific URI)
tx_id, offset = await outbox.get_position(session, "broker", uri="kafka://orders")

# Reset position (reprocess from beginning)
await outbox.set_position(session, "broker", uri="", transaction_id=0, offset=0)

# Skip ahead
await outbox.set_position(session, "broker", uri="kafka://orders", transaction_id=1000, offset=50)
```

## Schema

The `setup(session)` method creates:


### outbox table

```sql
CREATE TABLE outbox (
    "position" BIGSERIAL,
    "uri" VARCHAR(255) NOT NULL,
    "payload" BYTEA NOT NULL,
    "metadata" JSONB NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "transaction_id" xid8 NOT NULL,
    PRIMARY KEY ("transaction_id", "position")
);

CREATE INDEX outbox_position_idx ON outbox ("position");
CREATE INDEX outbox_uri_idx ON outbox ("uri");
CREATE UNIQUE INDEX outbox_message_id_uniq ON outbox (((metadata->>'message_id')::uuid));
```

### outbox_offsets table

```sql
CREATE TABLE outbox_offsets (
    "consumer_group" VARCHAR(255) NOT NULL,
    "uri" VARCHAR(255) NOT NULL DEFAULT '',
    "offset_acked" BIGINT NOT NULL DEFAULT 0,
    "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("consumer_group", "uri")
);
```

The composite primary key `(consumer_group, uri)` allows:
- `uri = ''` (empty string): Track position for ALL messages (default, backwards compatible)
- `uri = 'kafka://orders'`: Track position for this URI only


## Considerations


### Duplicate Delivery

Consumers must handle duplicates. Use `metadata.message_id` for deduplication:

```python
async def handle_message(message: OutboxMessage) -> None:
    message_id = message.metadata.get("message_id")
    if await is_already_processed(message_id):
        return  # Skip duplicate

    await process(message)
    await mark_as_processed(message_id)
```


### Message Ordering

- Within a single transaction: ordered by `position`
- Across transactions: ordered by `transaction_id`
- With URI filter: order preserved within that URI
- With partitioning: order preserved within each partition key (full URI)


### Table Growth

Processed messages should be archived or deleted periodically:

```sql
DELETE FROM outbox
WHERE transaction_id < (
    SELECT MIN(last_processed_transaction_id)
    FROM outbox_offsets
)
AND created_at < NOW() - INTERVAL '7 days';
```

### Polling vs Push

This implementation uses polling. For lower latency, consider:
- PostgreSQL `LISTEN`/`NOTIFY`
- `pg_logical` replication


## API Reference


### OutboxMessage

```python
@dataclass
class OutboxMessage:
    uri: str                     # Routing URI (e.g., 'kafka://orders')
    payload: bytes               # The message as it goes on the wire
    metadata: dict[str, Any]     # Must contain 'message_id' for idempotency
    created_at: str | None       # Auto-assigned by DB
    position: int | None         # Auto-assigned by DB
    transaction_id: int | None   # Auto-assigned by pg_current_xact_id()
```

### IOutbox Interface

| Method | Description |
|--------|-------------|
| `publish(session, message)` | Store message in outbox within current transaction |
| `dispatch(subscriber, ...)` | Dispatch next batch of messages |
| `run(subscriber, ...)` | Run continuous dispatching loop |
| `__aiter__()` | Async iterator for message streaming |
| `get_position(session, consumer_group, uri)` | Get current position for consumer group |
| `set_position(session, consumer_group, uri, transaction_id, offset)` | Set position for consumer group |
| `setup(session)` | Create tables and indexes |
| `cleanup(session)` | Cleanup resources |


### dispatch() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriber` | `ISubscriber` | required | Callback to handle each message |
| `consumer_group` | `str` | `''` | Consumer group identifier |
| `uri` | `str` | `''` | URI prefix filter (matches exact and `uri/*`) |
| `worker_id` | `int` | `0` | This worker's ID (0 to num_workers-1) |
| `num_workers` | `int` | `1` | Total number of workers for partitioning |


### run() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriber` | `ISubscriber` | required | Callback to handle each message |
| `consumer_group` | `str` | `''` | Consumer group identifier |
| `uri` | `str` | `''` | URI prefix filter |
| `process_id` | `int` | `0` | This process's ID (0 to num_processes-1) |
| `num_processes` | `int` | `1` | Total number of processes |
| `concurrency` | `int` | `1` | Number of coroutines in this process |
| `poll_interval` | `float` | `1.0` | Seconds to wait when no messages |
| `stop_event` | `Event` | `None` | For graceful shutdown |


### ISubscriber

```python
ISubscriber: TypeAlias = Callable[[OutboxMessage], Awaitable[None]]
```

## Notes

`tenant_id` is typed as `typing.Any` -- the actual type is determined
by the user's DDL schema (`varchar`, `integer`, etc.). The DDL
can include `REFERENCES` to enforce referential integrity.


## References

```{seealso}

- "[Domain Events in DDD](https://dckms.github.io/system-architecture/emacsway/it/ddd/tactical-design/domain-model/domain-events/domain-events-in-ddd.html)" by Ivan Zakrevsky
- "[.NET Microservices: Architecture for Containerized .NET Applications](https://learn.microsoft.com/en-us/dotnet/architecture/microservices/multi-container-microservice-net-applications/subscribe-events#designing-atomicity-and-resiliency-when-publishing-to-the-event-bus)" by Cesar de la Torre, Bill Wagner, Mike Rousos, chapter "Designing atomicity and resiliency when publishing to the event bus"
- "[Enterprise Integration Patterns: Designing, Building, and Deploying Messaging Solutions](https://www.enterpriseintegrationpatterns.com/)" by Gregor Hohpe, Bobby Woolf, "Chapter 10.Messaging Endpoints :: Transactional Client"
- "[Reactive Messaging Patterns with the Actor Model: Applications and Integration in Scala and Akka](https://kalele.io/books/)" by Vaughn Vernon, "Chapter 9. Message Endpoints :: Transactional Client/Actor"
- "[The Outbox Pattern](https://www.kamilgrzybek.com/blog/posts/the-outbox-pattern)" by Kamil Grzybek
- "[Handling Domain Event: Missing Part](https://www.kamilgrzybek.com/blog/posts/handling-domain-event-missing-part)" by Kamil Grzybek
- "[Ordering in Postgres Outbox](https://event-driven.io/en/ordering_in_postgres_outbox/)" by Oskar Dudycz
- "[Outbox, Inbox patterns and delivery guarantees explained](https://event-driven.io/en/outbox_inbox_patterns_and_delivery_guarantees_explained/)" by Oskar Dudycz

```

