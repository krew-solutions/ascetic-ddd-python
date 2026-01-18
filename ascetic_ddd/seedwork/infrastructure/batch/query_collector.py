"""Query collector for batching database operations."""
import typing
from types import TracebackType

from ascetic_ddd.deferred.deferred import Deferred
from ascetic_ddd.seedwork.infrastructure.session.interfaces import (
    IPgSession, IAsyncTransaction, Query, Params, Row,
)

from .interfaces import IMultiQuerier
from .multi_query import MultiQuery, AutoincrementMultiInsertQuery
from .utils import is_insert_query, is_autoincrement_insert_query


__all__ = ("QueryCollector", "ConnectionCollector", "CursorCollector")

# Type alias for query collector callable (mirrors IAsyncCursor.execute signature)
CollectQueryFn = typing.Callable[
    [Query, Params | None],
    Deferred[Row],
]


class CursorCollector:
    """
    Cursor that collects queries for batch execution.

    Implements IAsyncCursor interface to mimic real database cursor.
    Instead of executing queries immediately, stores them for batch processing.
    """

    def __init__(self, collect_query: CollectQueryFn):
        self._collect_query = collect_query
        self._last_result: Deferred[Row] | None = None

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> "CursorCollector":
        """
        Collect query for batch execution instead of executing immediately.

        Args:
            query: SQL query string with positional placeholders %s
            params: Sequence of parameter values
            prepare: Ignored (for interface compatibility)
            binary: Ignored (for interface compatibility)

        Returns:
            Self for chaining
        """
        self._last_result = self._collect_query(query, params)
        return self

    async def fetchone(self) -> Deferred[Row] | None:
        """
        Return deferred result for single row.

        Returns Deferred[Row] that will be resolved after batch evaluation.
        """
        return self._last_result

    async def fetchmany(self, size: int = 0) -> Deferred[list[Row]]:
        """
        Return deferred result for multiple rows.

        Returns Deferred[list[Row]] that will be resolved after batch evaluation.
        """
        result: Deferred[list[Row]] = Deferred()
        if self._last_result is not None:
            def on_resolve(row: Row) -> Exception | None:
                result.resolve([row] if row is not None else [])
                return None
            self._last_result.then(on_resolve, lambda e: None)
        else:
            result.resolve([])
        return result

    async def fetchall(self) -> Deferred[list[Row]]:
        """
        Return deferred result for all rows.

        Returns Deferred[list[Row]] that will be resolved after batch evaluation.
        """
        return await self.fetchmany()

    async def close(self) -> None:
        """No-op for batch cursor."""
        pass

    async def __aenter__(self) -> "CursorCollector":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()


class ConnectionCollector:
    """
    Connection that provides batch cursors.

    Implements IAsyncConnection interface to mimic real database connection.
    """

    def __init__(self, collect_query: CollectQueryFn):
        self._collect_query = collect_query

    def cursor(self, *args: typing.Any, **kwargs: typing.Any) -> CursorCollector:
        """Return a cursor collector for collecting queries."""
        return CursorCollector(self._collect_query)

    def transaction(
        self,
        savepoint_name: str | None = None,
        force_rollback: bool = False
    ) -> typing.AsyncContextManager["IAsyncTransaction"]:
        """Transactions not supported in batch mode."""
        raise NotImplementedError("Transactions not supported in batch collector")

    async def close(self) -> None:
        """No-op for batch connection."""
        pass

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool = False,
    ) -> CursorCollector:
        """Execute query through cursor."""
        cursor = self.cursor()
        await cursor.execute(query, params, prepare=prepare, binary=binary)
        return cursor

    async def __aenter__(self) -> "ConnectionCollector":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()


class QueryCollector:
    """
    Collects and batches database queries for efficient execution.

    Implements IPgSession interface to substitute real database session.
    Groups similar INSERT queries together and executes them in batches.
    This resolves N+1 query problems by combining multiple individual
    INSERT statements into bulk operations.

    Example usage:
        collector = QueryCollector()

        # Pass collector instead of real session to queries
        await query1.evaluate(collector)  # Collects INSERT
        await query2.evaluate(collector)  # Collects INSERT
        await query3.evaluate(collector)  # Collects INSERT

        # Execute all as single batch against real session
        await collector.evaluate(real_session)

        # Results are now available through deferred objects
    """

    def __init__(self):
        self._multi_query_map: dict[str, IMultiQuerier] = {}
        self._connection = ConnectionCollector(self._collect_query)

    @property
    def connection(self) -> ConnectionCollector:
        """Return batch connection for collecting queries."""
        return self._connection

    def _collect_query(
        self,
        query: Query,
        params: Params | None = None,
    ) -> Deferred[Row]:
        """
        Internal method to collect a query for batching.

        Args:
            query: SQL query string with positional placeholders %s
            params: Sequence of parameter values

        Returns:
            Deferred[Row] that will be resolved when batch is evaluated
        """
        query_str = query if isinstance(query, str) else query.decode()

        if query_str not in self._multi_query_map:
            if is_autoincrement_insert_query(query_str):
                self._multi_query_map[query_str] = AutoincrementMultiInsertQuery()
            elif is_insert_query(query_str):
                self._multi_query_map[query_str] = MultiQuery()

        if query_str in self._multi_query_map:
            return self._multi_query_map[query_str].execute(query, params)

        # For non-batchable queries, create immediately resolved result
        result: Deferred[Row] = Deferred()
        result.resolve(None)
        return result

    async def evaluate(self, session: IPgSession) -> None:
        """
        Execute all collected queries in batches against real session.

        Processes queries iteratively to handle nested queries that may
        be added during evaluation (e.g., when resolving auto-increment PKs
        triggers additional inserts via deferred callbacks).

        Args:
            session: Real database session to execute batched queries against
        """
        # Process queries iteratively - nested queries may be added during evaluation
        # (resolves N+1 query problem with auto-increment primary key)
        while self._multi_query_map:
            current_map = self._multi_query_map
            self._multi_query_map = {}

            for multi_query in current_map.values():
                await multi_query.evaluate(session)

    def clear(self) -> None:
        """Clear all collected queries."""
        self._multi_query_map.clear()

    def __len__(self) -> int:
        """Return the number of query types collected."""
        return len(self._multi_query_map)
