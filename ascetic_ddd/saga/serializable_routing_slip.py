"""Serializable forms of RoutingSlip and its constituents.

Plain data containers with no behavior. RoutingSlip itself stores activity
classes (not JSON-serializable); these dataclasses store activity *names*
(strings) instead, ready for transmission over a message bus.

The to_dict() / from_dict() helpers convert between snake_case Python
attributes and camelCase JSON keys, matching the wire format produced by
the Go implementation so that distributed sagas can interoperate across
languages.
"""

from dataclasses import dataclass, field
from typing import Any

from ascetic_ddd.saga.work_item_arguments import WorkItemArguments
from ascetic_ddd.saga.work_result import WorkResult


__all__ = (
    'SerializableRoutingSlip',
    'SerializableWorkItem',
    'SerializableWorkLog',
)


@dataclass
class SerializableWorkItem:
    """Serializable counterpart of WorkItem.

    Stores the activity type name (string) instead of the activity class
    itself, plus the same arguments dictionary.
    """

    activity_type_name: str
    arguments: WorkItemArguments

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dict with camelCase keys."""
        return {
            "activityTypeName": self.activity_type_name,
            "arguments": dict(self.arguments),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'SerializableWorkItem':
        """Reconstruct from a JSON-compatible dict with camelCase keys."""
        return cls(
            activity_type_name=data["activityTypeName"],
            arguments=WorkItemArguments(data["arguments"]),
        )


@dataclass
class SerializableWorkLog:
    """Serializable counterpart of WorkLog.

    Stores the activity type name (string) instead of the activity class
    itself, plus the same result dictionary.
    """

    activity_type_name: str
    result: WorkResult

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dict with camelCase keys."""
        return {
            "activityTypeName": self.activity_type_name,
            "result": dict(self.result),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'SerializableWorkLog':
        """Reconstruct from a JSON-compatible dict with camelCase keys."""
        return cls(
            activity_type_name=data["activityTypeName"],
            result=WorkResult(data["result"]),
        )


@dataclass
class SerializableRoutingSlip:
    """Serializable counterpart of RoutingSlip.

    Carries the same forward queue (next_work_items) and backward stack
    (completed_work_logs), but in name-keyed form ready for JSON.
    """

    completed_work_logs: list[SerializableWorkLog] = field(default_factory=list)
    next_work_items: list[SerializableWorkItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dict with camelCase keys."""
        return {
            "completedWorkLogs": [log.to_dict() for log in self.completed_work_logs],
            "nextWorkItems": [item.to_dict() for item in self.next_work_items],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'SerializableRoutingSlip':
        """Reconstruct from a JSON-compatible dict with camelCase keys."""
        return cls(
            completed_work_logs=[
                SerializableWorkLog.from_dict(d)
                for d in data.get("completedWorkLogs", [])
            ],
            next_work_items=[
                SerializableWorkItem.from_dict(d)
                for d in data.get("nextWorkItems", [])
            ],
        )
