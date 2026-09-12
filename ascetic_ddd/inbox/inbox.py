"""Inbox pattern implementation."""

import asyncio
import functools
import json
from typing import AsyncIterator, Any

from psycopg.types.json import Jsonb

from ascetic_ddd.inbox.interfaces import IInbox, ISubscriber
from ascetic_ddd.inbox.message import InboxMessage
from ascetic_ddd.inbox.partition_strategy import (
    IPartitionKeyStrategy,
    UriPartitionKeyStrategy,
)
from ascetic_ddd.session.interfaces import ISession, ISessionPool
from ascetic_ddd.session.pg_session import extract_connection
from ascetic_ddd.utils.json import JSONEncoder


__all__ = (
    'Inbox',
)


class Inbox(IInbox):
    """Inbox pattern implementation using PostgreSQL.

    Ensures idempotency and causal consistency for incoming integration messages.
    """
    _extract_connection = staticmethod(extract_connection)
    _table: str = 'inbox'
    _sequence: str = 'inbox_received_position_seq'

    def __init__(
            self,
            session_pool: ISessionPool,
            partition_key_strategy: IPartitionKeyStrategy | None = None,
    ):
        """Initialize Inbox.

        Args:
            session_pool: Pool for obtaining database sessions.
            partition_key_strategy: Strategy for computing partition key.
                Defaults to UriPartitionKeyStrategy.
        """
        self._session_pool = session_pool
        self._partition_key_strategy = partition_key_strategy or UriPartitionKeyStrategy()

    async def publish(self, message: InboxMessage) -> None:
        """Receive and store an incoming message.

        Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
        """
        async with self._session_pool.session() as session:
            async with session.atomic():
                await self._insert_message(session, message)

    async def dispatch(
            self,
            subscriber: ISubscriber,
            worker_id: int = 0,
            num_workers: int = 1,
    ) -> bool:
        """Process the next unprocessed message with satisfied dependencies.

        Args:
            subscriber: Callback to process the message.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers for partitioning.
        """
        async with self._session_pool.session() as session:
            async with session.atomic():
                message = await self._fetch_next_processable(
                    session, worker_id=worker_id, num_workers=num_workers
                )
                if message is None:
                    return False

                await subscriber(session, message)
                await self._mark_processed(session, message)
                return True

    def __aiter__(self) -> AsyncIterator[tuple[ISession, InboxMessage]]:
        """Return async iterator for continuous message processing.

        Usage:
            async for session, message in inbox:
                # Process message within transaction
                await handle_message(session, message)
                # Message is automatically marked as processed after yield

        The iterator runs indefinitely, polling for new messages.
        Use Ctrl+C or break to stop.
        """
        return self._iterate()

    async def _iterate(
            self, poll_interval: float = 1.0
    ) -> AsyncIterator[tuple[ISession, InboxMessage]]:
        """Async generator that yields (session, message) pairs.

        Args:
            poll_interval: Seconds to wait when no messages available.

        Yields:
            Tuple of (session, message) for each processable message.
            Message is marked as processed after the yield returns.
        """
        while True:
            async with self._session_pool.session() as session:
                async with session.atomic():
                    message = await self._fetch_next_processable(session)

                    if message is None:
                        # No processable messages, wait before polling again
                        await asyncio.sleep(poll_interval)
                        continue

                    # Yield to caller for processing, mark as processed after
                    try:
                        yield session, message
                    finally:
                        await self._mark_processed(session, message)

    async def run(
            self,
            subscriber: ISubscriber,
            process_id: int = 0,
            num_processes: int = 1,
            concurrency: int = 1,
            poll_interval: float = 1.0,
            stop_event: asyncio.Event | None = None,
    ) -> None:
        """Run message processing with partitioned workers.

        Each coroutine processes its own partitions:
          effective_id = process_id * concurrency + local_id
          effective_total = num_processes * concurrency

        Args:
            subscriber: Callback to process each message.
            process_id: This process's ID (0 to num_processes-1).
            num_processes: Total number of processes.
            concurrency: Number of coroutines within this process.
            poll_interval: Seconds to wait when no messages available.
            stop_event: Event to signal graceful shutdown.
        """
        if stop_event is None:
            stop_event = asyncio.Event()

        effective_total = num_processes * concurrency

        async def worker_loop(local_id: int):
            effective_id = process_id * concurrency + local_id
            while not stop_event.is_set():
                processed = await self.dispatch(
                    subscriber,
                    worker_id=effective_id,
                    num_workers=effective_total,
                )
                if not processed:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=poll_interval
                        )
                        break  # stop_event was set
                    except asyncio.TimeoutError:
                        pass  # Continue polling

        if concurrency == 1:
            await worker_loop(0)
        else:
            tasks = [asyncio.create_task(worker_loop(i)) for i in range(concurrency)]
            await asyncio.gather(*tasks)

    async def _insert_message(self, session: ISession, message: InboxMessage) -> None:
        """Insert message into inbox table."""
        sql = """
            INSERT INTO %s (
                tenant_id, stream_type, stream_id, stream_position,
                uri, payload, metadata
            ) VALUES (
                %%s, %%s, %%s, %%s, %%s, %%s, %%s
            )
            ON CONFLICT (tenant_id, stream_type, stream_id, stream_position) DO NOTHING
        """ % self._table

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(
                sql,
                (
                    message.tenant_id,
                    message.stream_type,
                    self._to_jsonb(message.stream_id),
                    message.stream_position,
                    message.uri,
                    self._to_jsonb(message.payload),
                    self._to_jsonb(message.metadata) if message.metadata else None,
                )
            )

    async def _fetch_next_processable(
            self,
            session: ISession,
            start_offset: int = 0,
            worker_id: int = 0,
            num_workers: int = 1,
    ) -> InboxMessage | None:
        """Find next message with satisfied dependencies.

        Args:
            session: Database session.
            start_offset: Offset to start searching from.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers for partitioning.

        Returns:
            Next processable message or None if no messages available.
        """
        offset = start_offset
        while True:
            message = await self._fetch_unprocessed_message(
                session, offset, worker_id=worker_id, num_workers=num_workers
            )
            if message is None:
                return None
            if await self._are_dependencies_satisfied(session, message):
                return message
            offset += 1

    async def _fetch_unprocessed_message(
            self,
            session: ISession,
            offset: int,
            worker_id: int = 0,
            num_workers: int = 1,
    ) -> InboxMessage | None:
        """Fetch first unprocessed message at given offset with row-level lock.

        Args:
            session: Database session.
            offset: Offset within the result set.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers for partitioning.
        """
        # Build partition filter.
        # hashtext() is signed and % keeps the sign of the dividend, so a
        # negative hash would match no worker; the sign bit is cleared.
        if num_workers > 1:
            partition_expr = self._partition_key_strategy.get_sql_expression()
            partition_filter = "AND (hashtext(%s) & 2147483647) %%%% %d = %d" % (
                partition_expr, num_workers, worker_id
            )
        else:
            partition_filter = ""

        sql = """
            SELECT
                tenant_id, stream_type, stream_id, stream_position,
                uri, payload, metadata,
                received_position, processed_position
            FROM %s
            WHERE processed_position IS NULL
            %s
            ORDER BY received_position ASC
            LIMIT 1 OFFSET %%s
            FOR UPDATE SKIP LOCKED
        """ % (self._table, partition_filter)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, (offset,))
            row = await cursor.fetchone()

        if row is None:
            return None

        return InboxMessage(
            tenant_id=row[0],
            stream_type=row[1],
            stream_id=row[2] if isinstance(row[2], dict) else json.loads(row[2]),
            stream_position=row[3],
            uri=row[4],
            payload=row[5] if isinstance(row[5], dict) else json.loads(row[5]),
            metadata=row[6] if row[6] is None or isinstance(row[6], dict) else json.loads(row[6]),
            received_position=row[7],
            processed_position=row[8],
        )

    async def _are_dependencies_satisfied(
            self, session: ISession, message: InboxMessage
    ) -> bool:
        """Check if all causal dependencies are processed."""
        dependencies = message.causal_dependencies
        if not dependencies:
            return True

        for dep in dependencies:
            if not await self._is_dependency_processed(session, dep):
                return False

        return True

    async def _is_dependency_processed(
            self, session: ISession, dependency: dict[str, Any]
    ) -> bool:
        """Check if a single dependency is processed."""
        sql = """
            SELECT 1 FROM %s
            WHERE tenant_id = %%s
              AND stream_type = %%s
              AND stream_id = %%s
              AND stream_position = %%s
              AND processed_position IS NOT NULL
            LIMIT 1
        """ % self._table

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(
                sql,
                (
                    dependency['tenant_id'],
                    dependency['stream_type'],
                    self._to_jsonb(dependency['stream_id']),
                    dependency['stream_position'],
                )
            )
            row = await cursor.fetchone()

        return row is not None

    async def _mark_processed(self, session: ISession, message: InboxMessage) -> None:
        """Mark message as processed with next sequence value."""
        sql = """
            UPDATE %s
            SET processed_position = nextval('%s')
            WHERE tenant_id = %%s
              AND stream_type = %%s
              AND stream_id = %%s
              AND stream_position = %%s
        """ % (self._table, self._sequence)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(
                sql,
                (
                    message.tenant_id,
                    message.stream_type,
                    self._to_jsonb(message.stream_id),
                    message.stream_position,
                )
            )

    @staticmethod
    def _to_jsonb(obj: dict) -> Jsonb:
        """Convert dict to Jsonb for psycopg."""
        dumps = functools.partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)

    async def setup(self, session: ISession) -> None:
        """Create inbox table and sequence if they don't exist."""
        await self._create_sequence(session)
        await self._create_table(session)

    async def _create_sequence(self, session: ISession) -> None:
        """Create sequence for received_position."""
        sql = "CREATE SEQUENCE IF NOT EXISTS %s" % self._sequence
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

    async def _create_table(self, session: ISession) -> None:
        """Create inbox table."""
        sql = """
            CREATE TABLE IF NOT EXISTS %s (
                tenant_id varchar(128) NOT NULL,
                stream_type varchar(128) NOT NULL,
                stream_id jsonb NOT NULL,
                stream_position integer NOT NULL,
                uri varchar(60) NOT NULL,
                payload jsonb NOT NULL,
                metadata jsonb NULL,
                received_position bigint NOT NULL UNIQUE DEFAULT nextval('%s'),
                processed_position bigint NULL,
                CONSTRAINT %s_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
            )
        """ % (self._table, self._sequence, self._table)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

    async def cleanup(self, session: ISession) -> None:
        """Cleanup resources (no-op for now)."""
        pass
