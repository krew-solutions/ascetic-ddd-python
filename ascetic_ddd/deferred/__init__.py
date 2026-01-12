"""Deferred pattern for handling asynchronous operations."""
from .deferred import Deferred, noop
from .interfaces import IDeferred, DeferredCallback

__all__ = [
    "IDeferred",
    "DeferredCallback",
    "Deferred",
    "noop",
]
