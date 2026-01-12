import datetime
import typing
import uuid

from .causal_dependency import CausalDependency
from .causal_dependency_exporter import CausalDependencyExporter
from .event_meta import IEventMetaExporter

__all__ = ("EventMetaExporter",)


class EventMetaExporter(IEventMetaExporter):
    def __init__(self) -> None:
        self.data: dict[str, typing.Any] = {
            "causal_dependencies": [],
        }

    def set_event_id(self, value: uuid.UUID | None) -> None:
        self.data["event_id"] = value

    def set_causation_id(self, value: uuid.UUID | None) -> None:
        self.data["causation_id"] = value

    def set_correlation_id(self, value: uuid.UUID | None) -> None:
        self.data["correlation_id"] = value

    def set_reason(self, value: str | None) -> None:
        self.data["reason"] = value

    def set_occurred_at(self, value: datetime.datetime | None) -> None:
        self.data["occurred_at"] = value

    def add_causal_dependency(self, value: CausalDependency) -> None:
        ex = CausalDependencyExporter()
        value.export(ex)
        self.data["causal_dependencies"].append(ex.data)
