# Transactional Inbox

```{index} Inbox, Transactional Inbox, Idempotency, Causal Consistency, Causal Dependencies
```

Transactional Inbox pattern for idempotent message processing with causal consistency and worker partitioning.


## Features

- **Idempotency**: Duplicate messages are ignored (same PK = same message)
- **Causal Consistency**: Messages are processed only after their causal dependencies
- **Worker Partitioning**: Messages are distributed across workers by partition key strategy
- **At-least-once delivery**: Messages are stored before processing


## Usage

```python
from ascetic_ddd.inbox import Inbox, InboxMessage

inbox = Inbox(session_pool)
async with session_pool.session() as session:
    async with session.atomic():
        await inbox.setup(session)
```


### Partition Key Strategies

Two strategies determine how messages are distributed across workers:


#### 1. UriPartitionKeyStrategy (default)

Partition by URI. Use when ordering is based on topic/partition from the broker.

```python
from ascetic_ddd.inbox import Inbox, UriPartitionKeyStrategy

inbox = Inbox(session_pool, partition_key_strategy=UriPartitionKeyStrategy())
```

The URI may contain a partition key suffix (like Outbox):
- `kafka://orders` — all messages go to one worker
- `kafka://orders/order-123` — distributed by hash of full URI


#### 2. StreamPartitionKeyStrategy

Partition by stream identity `(tenant_id, stream_type, stream_id)`. Use when messages have causal dependencies within a stream.

```python
from ascetic_ddd.inbox import Inbox, StreamPartitionKeyStrategy

inbox = Inbox(session_pool, partition_key_strategy=StreamPartitionKeyStrategy())
```

All messages for the same stream go to the same worker, preserving causal order.


### Publishing Messages

Receive messages from external source (e.g., message broker):

```python
await inbox.publish(InboxMessage(
    tenant_id="tenant1",
    stream_type="Order",
    stream_id={"id": "order-123"},
    stream_position=1,
    uri="kafka://orders/order-123",
    payload=b'{"type": "OrderCreated", "amount": 100}',
    metadata={
        "message_id": "uuid-123",
        "causal_dependencies": [
            {"tenant_id": "tenant1", "stream_type": "User", "stream_id": {"id": "user-1"}, "stream_position": 5}
        ]
    }
))
```


### Processing Messages


#### Option 1: dispatch() - Single Message

```python
async def handle_message(session, message: InboxMessage) -> None:
    event = deserialize(message.payload)
    await process_event(session, event)

# Process one message
has_message = await inbox.dispatch(handle_message)

# With partitioning (for manual worker management)
has_message = await inbox.dispatch(handle_message, worker_id=0, num_workers=3)
```


#### Option 2: run() - Continuous Loop

```python
stop_event = asyncio.Event()

# Single coroutine, single process (default)
await inbox.run(
    subscriber=handle_message,
    poll_interval=1.0,
    stop_event=stop_event,
)

# Multiple coroutines in single process (4 partitions)
await inbox.run(handle_message, concurrency=4)

# Multiple processes (run in separate processes, 3 partitions total)
# Process 0:
await inbox.run(handle_message, process_id=0, num_processes=3)
# Process 1:
await inbox.run(handle_message, process_id=1, num_processes=3)
# Process 2:
await inbox.run(handle_message, process_id=2, num_processes=3)

# Multiple processes with multiple coroutines (2 processes × 2 coroutines = 4 partitions)
# Process 0: handles partitions 0, 1
await inbox.run(handle_message, process_id=0, num_processes=2, concurrency=2)
# Process 1: handles partitions 2, 3
await inbox.run(handle_message, process_id=1, num_processes=2, concurrency=2)
```

Each coroutine processes its own partitions:
```
effective_id = process_id * concurrency + local_id
effective_total = num_processes * concurrency
```

#### Option 3: Async Iterator

```python
async for session, message in inbox:
    await handle_message(session, message)
    # Message is automatically marked as processed after yield
```

## Causal Dependencies

Messages can declare dependencies on other messages. A message is only processed after all its dependencies have been processed:

```python
metadata={
    "causal_dependencies": [
        {
            "tenant_id": "tenant1",
            "stream_type": "User",
            "stream_id": {"id": "user-1"},
            "stream_position": 5
        }
    ]
}
```

If dependencies are not satisfied, the Inbox skips to the next message and retries later.

**Important**: When using causal dependencies, use `StreamPartitionKeyStrategy` to ensure causally related messages go to the same worker.


## Database Schema

The Inbox uses PostgreSQL with the following schema:

```sql
CREATE SEQUENCE inbox_received_position_seq;

CREATE TABLE inbox (
    tenant_id varchar(128) NOT NULL,
    stream_type varchar(128) NOT NULL,
    stream_id jsonb NOT NULL,
    stream_position integer NOT NULL,
    uri varchar(255) NOT NULL,
    payload bytea NOT NULL,
    metadata jsonb NULL,
    received_position bigint NOT NULL UNIQUE DEFAULT nextval('inbox_received_position_seq'),
    processed_position bigint NULL,
    CONSTRAINT inbox_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
);
```

- **Primary Key**: `(tenant_id, stream_type, stream_id, stream_position)` ensures idempotency
- **received_position**: Order of message arrival
- **processed_position**: Set when message is processed (NULL = unprocessed)


## API Reference


### InboxMessage

```python
@dataclass
class InboxMessage:
    tenant_id: str                    # Tenant identifier
    stream_type: str                  # Event stream type (e.g., aggregate type)
    stream_id: dict[str, Any]         # Stream identifier (e.g., aggregate ID)
    stream_position: int              # Position in the stream
    uri: str                          # Routing URI (e.g., 'kafka://orders/order-123')
    payload: bytes                    # The message as it came off the wire
    metadata: dict[str, Any] | None   # Optional metadata (causal_dependencies, message_id)
    received_position: int | None     # Auto-assigned by DB
    processed_position: int | None    # Set when processed
```


### IInbox Interface

| Method | Description |
|--------|-------------|
| `publish(message)` | Store incoming message in inbox |
| `dispatch(subscriber, worker_id, num_workers)` | Process next message |
| `run(subscriber, ...)` | Run continuous processing loop |
| `__aiter__()` | Async iterator for message streaming |
| `setup(session)` | Create tables and sequences |
| `cleanup(session)` | Cleanup resources |


### dispatch() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriber` | `ISubscriber` | required | Callback to handle each message |
| `worker_id` | `int` | `0` | This worker's ID (0 to num_workers-1) |
| `num_workers` | `int` | `1` | Total number of workers for partitioning |


### run() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriber` | `ISubscriber` | required | Callback to handle each message |
| `process_id` | `int` | `0` | This process's ID (0 to num_processes-1) |
| `num_processes` | `int` | `1` | Total number of processes |
| `concurrency` | `int` | `1` | Number of coroutines in this process |
| `poll_interval` | `float` | `1.0` | Seconds to wait when no messages |
| `stop_event` | `Event` | `None` | For graceful shutdown |


### ISubscriber

```python
ISubscriber: TypeAlias = Callable[[ISession, InboxMessage], Awaitable]
```


### Partition Key Strategies

| Strategy | SQL Expression | Use Case |
|----------|---------------|----------|
| `UriPartitionKeyStrategy` | `hashtext(uri)` | Topic-based routing (default) |
| `StreamPartitionKeyStrategy` | `hashtext(tenant_id \|\| ':' \|\| stream_type \|\| ':' \|\| stream_id::text)` | Causal consistency within stream |


## Notes

`tenant_id` is typed as `typing.Any` -- the actual type is determined
by the user's DDL schema (`varchar`, `integer`, etc.). The DDL
can include `REFERENCES` to enforce referential integrity.


## References

```{seealso}

- "[Domain Events in DDD](https://dckms.github.io/system-architecture/emacsway/it/ddd/tactical-design/domain-model/domain-events/domain-events-in-ddd.html)" by Ivan Zakrevsky
- "[About the message race in terms of competing subscribers](https://dckms.github.io/system-architecture/emacsway/it/integration/asynchronous/message-ordering-in-competing-consumers.html)" by Ivan Zakrevsky
- "[Enterprise Integration Patterns: Designing, Building, and Deploying Messaging Solutions](https://www.enterpriseintegrationpatterns.com/)" by Gregor Hohpe, Bobby Woolf, "Chapter 10.Messaging Endpoints :: Transactional Client"
- "[Reactive Messaging Patterns with the Actor Model: Applications and Integration in Scala and Akka](https://kalele.io/books/)" by Vaughn Vernon, "Chapter 9. Message Endpoints :: Transactional Client/Actor"

```

