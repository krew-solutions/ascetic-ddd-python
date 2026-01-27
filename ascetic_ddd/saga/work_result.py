"""Work result - dictionary of results from activity execution."""

from typing import Any


__all__ = (
    'WorkResult',
)


class WorkResult(dict[str, Any]):
    """Dictionary containing results from an activity's work execution.

    Stores key-value pairs representing the outcome of DoWork(),
    such as reservation IDs, confirmation numbers, etc.
    """
    pass
