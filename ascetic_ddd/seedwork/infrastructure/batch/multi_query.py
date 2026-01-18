"""Multi-query batch implementations for INSERT operations."""
import typing
from abc import ABCMeta

from ascetic_ddd.deferred.deferred import Deferred
from ascetic_ddd.seedwork.infrastructure.session.interfaces import (
    IPgSession, Query, Params, Row,
)

from .interfaces import IMultiQuerier
from .utils import RE_INSERT_VALUES, convert_named_to_positional


__all__ = (
    "MultiQueryBase",
    "MultiQuery",
    "AutoincrementMultiInsertQuery",
)


class MultiQueryBase(metaclass=ABCMeta):
    """
    Base class for batching multiple INSERT queries into one.

    Collects individual INSERT queries and combines them into a single
    bulk INSERT with multiple VALUES clauses for better performance.

    Works with positional parameters %s:

    Example:
        Individual queries:
            INSERT INTO t (a, b) VALUES (%s, %s)  -- params: (1, 'x')
            INSERT INTO t (a, b) VALUES (%s, %s)  -- params: (2, 'y')

        Combined query:
            INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s)
            -- params: (1, 'x', 2, 'y')
    """

    def __init__(self):
        self._sql_template: str = ""
        self._values_pattern: str = ""
        self._params: list[typing.Sequence[typing.Any]] = []
        self._results: list[Deferred[Row]] = []

    def _build_sql(self) -> str:
        """Build the combined SQL query with duplicated VALUES clauses."""
        combined_values = ", ".join([self._values_pattern] * len(self._params))
        return RE_INSERT_VALUES.sub(f"VALUES {combined_values}", self._sql_template)

    def _merge_params(self) -> tuple[typing.Any, ...]:
        """Merge all params sequences into one tuple."""
        result: list[typing.Any] = []
        for params in self._params:
            result.extend(params)
        return tuple(result)

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Deferred[Row]:
        """
        Add a query to the batch.

        Args:
            query: SQL INSERT query with positional (%s) or named (%(name)s) placeholders
            params: Sequence or Mapping of parameter values

        Returns:
            Deferred[Row] that will be resolved when batch is evaluated
        """
        query_str = query if isinstance(query, str) else query.decode()

        # Convert named params to positional if needed
        if isinstance(params, typing.Mapping):
            query_str, params = convert_named_to_positional(query_str, params)

        # Store template on first call
        if not self._sql_template:
            self._sql_template = query_str
            match = RE_INSERT_VALUES.search(query_str)
            if match:
                self._values_pattern = match.group(1)

        # Store parameters
        if params is None:
            self._params.append(())
        else:
            self._params.append(params)

        # Create and store deferred result
        result: Deferred[Row] = Deferred()
        self._results.append(result)
        return result


class MultiQuery(MultiQueryBase, IMultiQuerier):
    """
    Multi-query implementation for INSERT without RETURNING.

    Batches multiple INSERT queries and resolves all deferreds with None
    since no values are returned from bulk INSERT without RETURNING.
    """

    async def evaluate(self, session: IPgSession) -> None:
        """
        Execute the batched INSERT query.

        Args:
            session: Database session with execute method
        """
        if not self._params:
            return

        sql = self._build_sql()
        params = self._merge_params()
        await session.connection.execute(sql, params)

        # Resolve all deferred results with None (no RETURNING)
        for deferred in self._results:
            deferred.resolve(None)


class AutoincrementMultiInsertQuery(MultiQueryBase, IMultiQuerier):
    """
    Multi-query implementation for INSERT with RETURNING (auto-increment PK).

    Batches multiple INSERT queries with RETURNING clause.
    Each deferred result is resolved with its corresponding row.
    """

    async def evaluate(self, session: IPgSession) -> None:
        """
        Execute the batched INSERT query with RETURNING.

        Args:
            session: Database session with fetch_all method
        """
        if not self._params:
            return

        sql = self._build_sql()
        params = self._merge_params()
        cursor = await session.connection.execute(sql, params)
        rows = await cursor.fetchall()

        # Resolve each deferred result with its row
        for i, row in enumerate(rows):
            if i < len(self._results):
                self._results[i].resolve(row)
