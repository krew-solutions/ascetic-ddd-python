"""Work item arguments - dictionary of input arguments for activity."""

from typing import Any


__all__ = (
    'WorkItemArguments',
)


class WorkItemArguments(dict[str, Any]):
    """Dictionary containing input arguments for an activity.

    Stores key-value pairs representing the parameters needed
    by an activity to perform its work, such as vehicle type,
    room type, destination, etc.
    """
    pass
