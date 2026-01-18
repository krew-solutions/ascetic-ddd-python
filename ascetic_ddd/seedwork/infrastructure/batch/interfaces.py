"""Batch query interfaces."""
from abc import ABCMeta, abstractmethod

from ascetic_ddd.deferred.deferred import Deferred
from ascetic_ddd.seedwork.infrastructure.session.interfaces import (
    IPgSession, Query, Params, Row,
)


__all__ = (
    "IQueryEvaluator",
    "IMultiQuerier",
)


class IQueryEvaluator(metaclass=ABCMeta):
    """Interface for query evaluation."""

    @abstractmethod
    async def evaluate(self, session: IPgSession) -> None:
        """Evaluate collected queries against the database session."""
        raise NotImplementedError


class IMultiQuerier(IQueryEvaluator, metaclass=ABCMeta):
    """Interface for multi-query batch operations."""

    @abstractmethod
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
            query: SQL query string with positional placeholders %s
            params: Sequence of parameter values

        Returns:
            Deferred[Row] that will be resolved when batch is evaluated
        """
        raise NotImplementedError
