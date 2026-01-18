"""Batch query processing module."""
from .interfaces import (
    IQueryEvaluator,
    IMultiQuerier,
)
from .utils import (
    is_insert_query,
    is_autoincrement_insert_query,
)
from .multi_query import (
    MultiQueryBase,
    MultiQuery,
    AutoincrementMultiInsertQuery,
)
from .query_collector import QueryCollector, ConnectionCollector, CursorCollector


__all__ = (
    # Interfaces
    "IQueryEvaluator",
    "IMultiQuerier",
    # Utils
    "is_insert_query",
    "is_autoincrement_insert_query",
    # Multi-query
    "MultiQueryBase",
    "MultiQuery",
    "AutoincrementMultiInsertQuery",
    # Query collector
    "QueryCollector",
    "ConnectionCollector",
    "CursorCollector",
)
