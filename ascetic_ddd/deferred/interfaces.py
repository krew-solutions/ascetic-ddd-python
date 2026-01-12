"""Deferred pattern interfaces."""
from typing import Any, Callable, Generic, Protocol, TypeVar

T = TypeVar("T")

DeferredCallback = Callable[[T], Exception | None]
"""Callback function that processes a value and may return an error."""


class IDeferred(Protocol[T]):
    """Interface for deferred operations (similar to Promise)."""

    def then(
        self,
        on_success: DeferredCallback[T],
        on_error: DeferredCallback[Exception],
    ) -> "IDeferred[Any]":
        """
        Register callbacks for success and error cases.

        Args:
            on_success: Callback to execute on successful resolution
            on_error: Callback to execute on rejection

        Returns:
            New Deferred for chaining
        """
        ...
