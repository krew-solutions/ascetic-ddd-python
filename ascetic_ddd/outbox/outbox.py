"""PostgreSQL implementation of the Transactional Outbox pattern."""

import asyncio
import functools
import json
import typing

from psycopg.types.json import Jsonb

from ascetic_ddd.outbox.interfaces import IOutbox, ISubscriber
from ascetic_ddd.outbox.message import OutboxMessage
from ascetic_ddd.session.interfaces import ISession, ISessionPool
from ascetic_ddd.session.pg_session import extract_connection
from ascetic_ddd.utils.json import JSONEncoder


__all__ = ('Outbox',)


class Outbox(IOutbox):
    """PostgreSQL implementation of the Transactional Outbox pattern.

    Uses transaction_id (xid8) for correct ordering across concurrent transactions.
    See init.sql for schema and detailed documentation.
    """
    _extract_connection = staticmethod(extract_connection)
    _session_pool: ISessionPool
    _outbox_table: str
    _offsets_table: str
    _batch_size: int

    def __init__(
            self,
            session_pool: ISessionPool,
            outbox_table: str = 'outbox',
            offsets_table: str = 'outbox_offsets',
            batch_size: int = 100
    ):
        self._session_pool = session_pool
        self._outbox_table = outbox_table
        self._offsets_table = offsets_table
        self._batch_size = batch_size

    async def publish(
            self,
            session: 'ISession',
            message: 'OutboxMessage'
    ) -> None:
        """Store a message in the outbox within the current transaction."""
        sql = """
            INSERT INTO %s (uri, payload, metadata, transaction_id)
            VALUES (%%(uri)s, %%(payload)s, %%(metadata)s, pg_current_xact_id())
        """ % (self._outbox_table,)

        params = {
            'uri': message.uri,
            'payload': self._to_jsonb(message.payload),
            'metadata': self._to_jsonb(message.metadata),
        }

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, params)

    async def dispatch(
            self,
            subscriber: 'ISubscriber',
            consumer_group: str = '',
            uri: str = '',
            worker_id: int = 0,
            num_workers: int = 1
    ) -> bool:
        """Dispatch the next batch of unprocessed messages.

        Args:
            subscriber: Message handler.
            consumer_group: Consumer group name.
            uri: Optional URI prefix filter. If empty, processes all URIs.
                 If specified, processes messages with exact URI or URI/* prefix.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers for partitioning.
        """
        # Each worker tracks its own offset
        effective_consumer_group = (
            "%s:%d" % (consumer_group, worker_id)
            if num_workers > 1 else consumer_group
        )

        async with self._session_pool.session() as session:
            await self._ensure_consumer_group(session, effective_consumer_group, uri)

        async with self._session_pool.session() as session:
            async with session.atomic() as tx_session:
                # FOR UPDATE blocks if another dispatcher holds the lock
                messages = await self._fetch_messages(
                    tx_session, effective_consumer_group, uri,
                    worker_id=worker_id, num_workers=num_workers
                )

                if not messages:
                    return False

                # Process each message
                for message in messages:
                    await subscriber(message)

                # Update consumer position to the last message
                last = messages[-1]
                assert last.transaction_id is not None
                assert last.position is not None
                await self._ack_message(tx_session, effective_consumer_group, uri, last.transaction_id, last.position)

                return True

    def __aiter__(self) -> typing.AsyncIterator['OutboxMessage']:
        """Return async iterator for continuous message dispatching."""
        return self._iterate()

    async def _iterate(
            self,
            consumer_group: str = '',
            uri: str = '',
            worker_id: int = 0,
            num_workers: int = 1,
            poll_interval: float = 1.0
    ) -> typing.AsyncGenerator['OutboxMessage', None]:
        """Async generator that yields messages continuously."""
        # Ensure consumer group exists (once, before loop)
        async with self._session_pool.session() as session:
            await self._ensure_consumer_group(session, consumer_group, uri)

        while True:
            messages: list[OutboxMessage] = []

            async with self._session_pool.session() as session:
                async with session.atomic() as tx_session:
                    messages = await self._fetch_messages(
                        tx_session, consumer_group, uri,
                        worker_id=worker_id, num_workers=num_workers
                    )

                    for message in messages:
                        yield message
                        # Ack after each message is processed
                        assert message.transaction_id is not None
                        assert message.position is not None
                        await self._ack_message(
                            tx_session, consumer_group, uri,
                            message.transaction_id, message.position
                        )

            if not messages:
                await asyncio.sleep(poll_interval)

    async def run(
            self,
            subscriber: 'ISubscriber',
            consumer_group: str = '',
            uri: str = '',
            process_id: int = 0,
            num_processes: int = 1,
            concurrency: int = 1,
            poll_interval: float = 1.0,
            stop_event: asyncio.Event | None = None,
    ) -> None:
        """Run message dispatching loop.

        Each coroutine processes its own partitions:
          effective_id = process_id * concurrency + local_id
          effective_total = num_processes * concurrency

        Args:
            subscriber: Message handler.
            consumer_group: Consumer group name.
            uri: Optional URI prefix filter. If empty, processes all URIs.
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
                has_messages = await self.dispatch(
                    subscriber, consumer_group, uri,
                    worker_id=effective_id, num_workers=effective_total
                )
                if not has_messages:
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

    async def get_position(
            self,
            session: 'ISession',
            consumer_group: str = '',
            uri: str = ''
    ) -> tuple[int, int]:
        """Get current position for a consumer group (optionally filtered by uri)."""
        sql = """
            SELECT last_processed_transaction_id, offset_acked
            FROM %s
            WHERE consumer_group = %%(consumer_group)s AND uri = %%(uri)s
        """ % (self._offsets_table,)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, {'consumer_group': consumer_group, 'uri': uri})
            row = await cursor.fetchone()
            if row is None:
                return (0, 0)
            return (int(row[0]), row[1])

    async def set_position(
            self,
            session: 'ISession',
            consumer_group: str,
            uri: str,
            transaction_id: int,
            offset: int
    ) -> None:
        """Set position for a consumer group (optionally filtered by uri)."""
        sql = """
            INSERT INTO %s (consumer_group, uri, offset_acked, last_processed_transaction_id, updated_at)
            VALUES (%%(consumer_group)s, %%(uri)s, %%(offset)s, %%(transaction_id)s, CURRENT_TIMESTAMP)
            ON CONFLICT (consumer_group, uri) DO UPDATE SET
                offset_acked = EXCLUDED.offset_acked,
                last_processed_transaction_id = EXCLUDED.last_processed_transaction_id,
                updated_at = EXCLUDED.updated_at
        """ % (self._offsets_table,)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, {
                'consumer_group': consumer_group,
                'uri': uri,
                'offset': offset,
                'transaction_id': str(transaction_id),
            })

    # Private methods

    async def _ensure_consumer_group(self, session: 'ISession', consumer_group: str, uri: str = '') -> None:
        """Ensure consumer group exists with zero position.

        Required for FOR UPDATE locking to work (can't lock non-existent row).
        """
        sql = """
            INSERT INTO %s (consumer_group, uri, offset_acked, last_processed_transaction_id)
            VALUES (%%(consumer_group)s, %%(uri)s, 0, '0')
            ON CONFLICT DO NOTHING
        """ % (self._offsets_table,)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, {'consumer_group': consumer_group, 'uri': uri})

    async def _fetch_messages(
            self,
            session: 'ISession',
            consumer_group: str,
            uri: str = '',
            worker_id: int = 0,
            num_workers: int = 1
    ) -> list['OutboxMessage']:
        """Fetch next batch of messages with FOR UPDATE lock.

        Uses subquery wrapper to avoid query planner mis-estimation with ORDER BY + LIMIT.
        See watermill-sql comments for details.

        Note: _ensure_consumer_group must be called before this method.

        Args:
            session: Database session.
            consumer_group: Consumer group name.
            uri: Optional URI prefix filter. If empty, fetches all URIs.
                 If specified, fetches messages with exact URI or URI/* prefix.
            worker_id: This worker's ID (0 to num_workers-1).
            num_workers: Total number of workers for partitioning.
        """
        # Build uri filter clause (prefix matching)
        if uri:
            uri_filter = "AND (uri = %(uri)s OR uri LIKE %(uri_prefix)s)"
        else:
            uri_filter = ""

        # Build partition clause.
        # hashtext() is signed and % keeps the sign of the dividend, so a
        # negative hash would match no worker; the sign bit is cleared.
        if num_workers > 1:
            partition_filter = "AND (hashtext(uri) & 2147483647) %% %(num_workers)s = %(worker_id)s"
        else:
            partition_filter = ""

        # Query with subquery wrapper for query planner optimization
        sql = """
            SELECT * FROM (
                WITH last_processed AS (
                    SELECT offset_acked, last_processed_transaction_id
                    FROM %s
                    WHERE consumer_group = %%(consumer_group)s AND uri = %%(uri)s
                    FOR UPDATE
                )
                SELECT "position", transaction_id, uri, payload, metadata, created_at
                FROM %s
                WHERE (
                    (transaction_id = (SELECT last_processed_transaction_id FROM last_processed)
                     AND "position" > (SELECT offset_acked FROM last_processed))
                    OR
                    (transaction_id > (SELECT last_processed_transaction_id FROM last_processed))
                )
                AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                %s
                %s
            ) AS messages
            ORDER BY transaction_id ASC, "position" ASC
            LIMIT %d
        """ % (self._offsets_table, self._outbox_table, uri_filter, partition_filter, self._batch_size)

        params = {
            'consumer_group': consumer_group,
            'uri': uri,
            'uri_prefix': uri + '/%' if uri else '',
            'worker_id': worker_id,
            'num_workers': num_workers,
        }

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()

        return [self._row_to_message(row) for row in rows]

    async def _ack_message(
            self,
            session: 'ISession',
            consumer_group: str,
            uri: str,
            transaction_id: int,
            position: int
    ) -> None:
        """Acknowledge message by updating consumer position."""
        sql = """
            INSERT INTO %s (consumer_group, uri, offset_acked, last_processed_transaction_id, updated_at)
            VALUES (%%(consumer_group)s, %%(uri)s, %%(position)s, %%(transaction_id)s, CURRENT_TIMESTAMP)
            ON CONFLICT (consumer_group, uri) DO UPDATE SET
                offset_acked = EXCLUDED.offset_acked,
                last_processed_transaction_id = EXCLUDED.last_processed_transaction_id,
                updated_at = EXCLUDED.updated_at
        """ % (self._offsets_table,)

        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql, {
                'consumer_group': consumer_group,
                'uri': uri,
                'position': position,
                'transaction_id': str(transaction_id),
            })

    def _row_to_message(self, row: tuple) -> 'OutboxMessage':
        """Convert database row to OutboxMessage."""
        position, transaction_id, uri, payload, metadata, created_at = row
        return OutboxMessage(
            uri=uri,
            payload=payload if isinstance(payload, dict) else {},
            metadata=metadata if isinstance(metadata, dict) else {},
            created_at=str(created_at) if created_at else None,
            position=position,
            transaction_id=int(transaction_id) if transaction_id else None,
        )

    @staticmethod
    def _to_jsonb(obj: dict) -> Jsonb:
        """Convert dict to Jsonb for psycopg."""
        dumps = functools.partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)

    async def setup(self, session: ISession) -> None:
        """Initialize the outbox (create tables, sequences, indexes)."""
        await self._create_outbox_table(session)
        await self._create_offsets_table(session)

    async def _create_outbox_table(self, session: 'ISession') -> None:
        """Create outbox table with indexes."""
        sql = """
            CREATE TABLE IF NOT EXISTS %s (
                "position" BIGSERIAL,
                "uri" VARCHAR(255) NOT NULL,
                "payload" JSONB NOT NULL,
                "metadata" JSONB NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "transaction_id" xid8 NOT NULL,
                PRIMARY KEY ("transaction_id", "position")
            )
        """ % (self._outbox_table,)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

        # Index for position-based queries
        sql = """
            CREATE INDEX IF NOT EXISTS %s_position_idx ON %s ("position")
        """ % (self._outbox_table, self._outbox_table)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

        # Index for uri filtering
        sql = """
            CREATE INDEX IF NOT EXISTS %s_uri_idx ON %s ("uri")
        """ % (self._outbox_table, self._outbox_table)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

        # Unique index on event_id for idempotency
        sql = """
            CREATE UNIQUE INDEX IF NOT EXISTS %s_event_id_uniq
            ON %s (((metadata->>'event_id')::uuid))
        """ % (self._outbox_table, self._outbox_table)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

    async def _create_offsets_table(self, session: 'ISession') -> None:
        """Create offsets table for consumer groups.

        The composite PK (consumer_group, uri) allows:
        - uri='' (empty string): track position for all URIs (default behavior)
        - uri='kafka://orders': track position for specific URI only
        """
        sql = """
            CREATE TABLE IF NOT EXISTS %s (
                "consumer_group" VARCHAR(255) NOT NULL,
                "uri" VARCHAR(255) NOT NULL DEFAULT '',
                "offset_acked" BIGINT NOT NULL DEFAULT 0,
                "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',
                "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY ("consumer_group", "uri")
            )
        """ % (self._offsets_table,)
        async with self._extract_connection(session).cursor() as cursor:
            await cursor.execute(sql)

    async def cleanup(self, session: ISession) -> None:
        """Cleanup resources."""
        pass
