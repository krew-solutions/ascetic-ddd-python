"""
Deferred pattern implementation.

Simplified version of:
- https://github.com/emacsway/store/blob/devel/polyfill.js#L199
- https://github.com/emacsway/go-promise
"""
from typing import Any, Generic, TypeVar

from .interfaces import IDeferred, DeferredCallback

T = TypeVar("T")


def noop(_: T) -> None:
    """No-operation callback."""
    return None


class _Handler(Generic[T]):
    """Internal handler for deferred callbacks."""

    def __init__(
        self,
        on_success: DeferredCallback[T],
        on_error: DeferredCallback[Exception],
        next_deferred: "Deferred[Any]",
    ):
        self.on_success = on_success
        self.on_error = on_error
        self.next = next_deferred


class Deferred(Generic[T]):
    """
    Implementation of the Deferred pattern.

    Provides a way to handle asynchronous operations with callbacks
    for both success and error cases, similar to JavaScript Promises.
    """

    def __init__(self):
        self._value: T | None = None
        self._err: Exception | None = None
        self._occurred_errors: list[Exception] = []
        self._is_resolved = False
        self._is_rejected = False
        self._handlers: list[_Handler[T]] = []

    def resolve(self, value: T) -> None:
        """
        Resolve the deferred with a value.

        Triggers all registered success handlers.

        Args:
            value: The value to resolve with
        """
        self._value = value
        self._is_resolved = True
        for handler in self._handlers:
            self._resolve_handler(handler)

    def reject(self, err: Exception) -> None:
        """
        Reject the deferred with an error.

        Triggers all registered error handlers.

        Args:
            err: The error to reject with
        """
        self._err = err
        self._is_rejected = True
        for handler in self._handlers:
            self._reject_handler(handler)

    def then(
        self,
        on_success: DeferredCallback[T],
        on_error: DeferredCallback[Exception],
    ) -> IDeferred[Any]:
        """
        Register callbacks for success and error cases.

        Args:
            on_success: Callback to execute on successful resolution
            on_error: Callback to execute on rejection

        Returns:
            New Deferred for chaining
        """
        next_deferred = Deferred[Any]()
        handler = _Handler(on_success, on_error, next_deferred)
        self._handlers.append(handler)

        if self._is_resolved:
            self._resolve_handler(handler)
        elif self._is_rejected:
            self._reject_handler(handler)

        return next_deferred

    def _resolve_handler(self, handler: _Handler[T]) -> None:
        """
        Execute success handler.

        If handler returns an error, reject the next deferred.
        Otherwise, resolve the next deferred.

        Args:
            handler: The handler to execute
        """
        err = handler.on_success(self._value)
        if err is None:
            handler.next.resolve(True)
        else:
            self._occurred_errors.append(err)
            handler.next.reject(err)

    def _reject_handler(self, handler: _Handler[T]) -> None:
        """
        Execute error handler.

        If handler returns an error, propagate it to the next deferred.

        Args:
            handler: The handler to execute
        """
        err = handler.on_error(self._err)
        if err is not None:
            self._occurred_errors.append(err)
            handler.next.reject(err)

    def occurred_err(self) -> list[Exception]:
        """
        Collect all errors that occurred during execution.

        Recursively collects errors from the entire chain of deferreds.

        Returns:
            List of all exceptions that occurred
        """
        errors = self._occurred_errors.copy()
        for handler in self._handlers:
            nested_errors = handler.next.occurred_err()
            if nested_errors:
                errors.extend(nested_errors)
        return errors
