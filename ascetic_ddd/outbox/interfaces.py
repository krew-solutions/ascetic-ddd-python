"""Interfaces for the Transactional Outbox pattern.

The Outbox Pattern solves the dual-write problem: when a business operation needs to
both update the database and publish messages to external systems (message brokers,
other services), you cannot guarantee atomicity with traditional approaches.

Key Concepts:
------------

1. **Transactional Guarantee**:
   Messages are saved to the Outbox table within the SAME database transaction as
   the business state changes. This ensures that either both the state change AND
   the message are persisted, or neither is.

2. **At-Least-Once Delivery**:
   A separate dispatcher process reads unpublished messages and sends them to
   external systems. If the dispatcher crashes after publishing but before marking
   as processed, the message will be resent. Consumers MUST be idempotent.

3. **Ordering with transaction_id (xid8)**:
   PostgreSQL's SERIAL/BIGSERIAL doesn't guarantee ordering in concurrent transactions.
   Example: TX1 gets position=1, TX2 gets position=2, but TX2 commits first.
   A consumer reading position > 0 would see message 2 but miss message 1.

   Solution: Use `transaction_id` (xid8 type) + `position` for ordering:
   - `transaction_id` = pg_current_xact_id() captures the transaction ID
   - Read only messages where transaction_id < pg_snapshot_xmin(pg_current_snapshot())
     (i.e., only from COMMITTED transactions visible to all)
   - Order by (transaction_id, position) for deterministic ordering

4. **Consumer Groups**:
   Multiple consumers can independently track their position in the outbox.
   Each consumer group maintains its own (last_processed_transaction_id, offset_acked).

5. **Visibility Rule**:
   `transaction_id < pg_snapshot_xmin(pg_current_snapshot())` ensures we only read
   messages from transactions that are fully committed and visible to all sessions.
   This prevents reading messages from in-flight transactions.

Problems and Considerations:
---------------------------

1. **Duplicate Delivery**: Consumers must handle duplicates (use metadata.message_id for dedup).

2. **Message Ordering**: Within a single transaction, messages are ordered by position.
   Across transactions, they're ordered by transaction_id.

3. **Read Model Rebuilding**: When rebuilding read models, you may need to skip
   already-processed events. Track consumer position carefully.

4. **Table Growth**: Processed messages should be archived or deleted periodically.

5. **Polling vs Push**: This implementation uses polling. For lower latency, consider
   PostgreSQL LISTEN/NOTIFY or pg_logical replication.

References:
----------
- https://www.kamilgrzybek.com/blog/posts/the-outbox-pattern
- https://www.kamilgrzybek.com/blog/posts/handling-domain-event-missing-part
- https://event-driven.io/en/ordering_in_postgres_outbox/
- https://event-driven.io/en/outbox_inbox_patterns_and_டelivery_guarantees_explained/
"""

from abc import ABCMeta, abstractmethod
from typing import AsyncIterator, TypeAlias, Callable, Awaitable, Optional, Any

from ascetic_ddd.outbox.message import OutboxMessage
from ascetic_ddd.session.interfaces import ISession


__all__ = (
    'IOutbox',
    'ISubscriber',
)


# Callback for handling messages from the outbox
ISubscriber: TypeAlias = Callable[[OutboxMessage], Awaitable[None]]


class IOutbox(metaclass=ABCMeta):
    """Interface for the Transactional Outbox pattern.

    The Outbox pattern ensures:
    - Atomicity: Messages are persisted in the same transaction as business state
    - At-least-once delivery: Messages will be delivered at least once
    - Ordering: Messages are delivered in transaction commit order

    Typical usage flow:
    1. Business logic saves state changes
    2. Business logic calls outbox.publish() in the same transaction
    3. Transaction commits (both state and message are persisted atomically)
    4. Dispatcher calls outbox.dispatch() to send messages to external systems
    """

    @abstractmethod
    async def publish(
            self,
            session: 'ISession',
            message: 'OutboxMessage'
    ) -> None:
        """Store a message in the outbox within the current transaction.

        IMPORTANT: This must be called within an active database transaction.
        The message will only become visible to dispatchers after the transaction
        commits successfully.

        The transaction_id is automatically captured using pg_current_xact_id().

        Args:
            session: Database session with active transaction.
            message: The message to store in the outbox.
        """
        raise NotImplementedError

    @abstractmethod
    async def dispatch(
            self,
            subscriber: 'ISubscriber',
            consumer_group: str = '',
            uri: str = '',
            worker_id: int = 0,
            num_workers: int = 1
    ) -> bool:
        """Dispatch the next batch of unprocessed messages.

        Reads messages that:
        - Have transaction_id < pg_snapshot_xmin(pg_current_snapshot())
          (only from committed, visible transactions)
        - Are after the consumer group's last processed position
        - Match the URI filter (if specified)
        - Are assigned to this worker (hashtext(uri) % num_workers = worker_id)

        For each message, calls the subscriber callback. After successful
        processing of the batch, updates the consumer group's position.

        Uses FOR UPDATE to lock the consumer group row, preventing concurrent
        dispatchers from processing the same messages.

        URI and Partitioning:
        - uri="kafka://orders" matches both "kafka://orders" and "kafka://orders/*"
        - Messages are distributed across workers by hash(full_uri) % num_workers
        - This ensures all messages with the same URI go to the same worker,
          preserving ordering within a partition key
        - When num_workers > 1, consumer_group is extended with worker_id
          ("broker" becomes "broker:0", "broker:1", etc.) for offset isolation

        Args:
            subscriber: Callback to handle each message.
            consumer_group: Consumer group identifier (empty string for default).
            uri: Optional URI prefix filter. If empty, processes all URIs.
                 If specified, processes messages with exact URI or URI/* prefix.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers. Messages are distributed
                         by hashtext(uri) % num_workers.

        Returns:
            True if messages were dispatched, False if no messages available.
        """
        raise NotImplementedError

    @abstractmethod
    def __aiter__(self) -> AsyncIterator['OutboxMessage']:
        """Return async iterator for continuous message dispatching.

        Usage:
            async for message in outbox:
                await publish_to_broker(message)
                # Message position is updated after yield returns

        The iterator:
        - Runs indefinitely, polling for new messages
        - Yields messages from committed transactions
        - Updates consumer position after the yield returns
        - Uses FOR UPDATE SKIP LOCKED for concurrent dispatcher support

        Yields:
            OutboxMessage for each dispatchable message.
        """
        raise NotImplementedError

    @abstractmethod
    async def run(
            self,
            subscriber: 'ISubscriber',
            consumer_group: str = '',
            uri: str = '',
            process_id: int = 0,
            num_processes: int = 1,
            concurrency: int = 1,
            poll_interval: float = 1.0
    ) -> None:
        """Run message dispatching loop.

        Each coroutine processes its own partitions:
          effective_id = process_id * concurrency + local_id
          effective_total = num_processes * concurrency

        Example with 2 processes, 2 coroutines each (4 total partitions):
          Process 0: coroutines handle partitions 0, 1
          Process 1: coroutines handle partitions 2, 3

        Args:
            subscriber: Callback to handle each message.
            consumer_group: Consumer group identifier.
            uri: Optional URI prefix filter. If empty, processes all URIs.
            process_id: This process's ID (0 to num_processes-1).
            num_processes: Total number of processes.
            concurrency: Number of coroutines within this process.
            poll_interval: Seconds to wait when no messages available.
        """
        raise NotImplementedError

    @abstractmethod
    async def setup(self, session: 'ISession') -> None:
        """Initialize the outbox (create tables, sequences, indexes)."""
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, session: 'ISession') -> None:
        """Cleanup resources."""
        raise NotImplementedError

    @abstractmethod
    async def get_position(
            self,
            session: 'ISession',
            consumer_group: str = '',
            uri: str = ''
    ) -> tuple[int, int]:
        """Get current position for a consumer group.

        Args:
            session: Database session.
            consumer_group: Consumer group identifier.
            uri: Optional URI filter. Position is tracked per (consumer_group, uri).

        Returns:
            Tuple of (last_processed_transaction_id, offset_acked).
        """
        raise NotImplementedError

    @abstractmethod
    async def set_position(
            self,
            session: 'ISession',
            consumer_group: str,
            uri: str,
            transaction_id: int,
            offset: int
    ) -> None:
        """Set position for a consumer group.

        Useful for:
        - Resetting position to reprocess messages
        - Skipping ahead to ignore old messages
        - Initializing position for new consumer groups

        Args:
            session: Database session.
            consumer_group: Consumer group identifier.
            uri: URI filter. Position is tracked per (consumer_group, uri).
                 Use empty string for "all URIs" mode.
            transaction_id: Transaction ID to set as last processed.
            offset: Offset to set as acknowledged.
        """
        raise NotImplementedError
