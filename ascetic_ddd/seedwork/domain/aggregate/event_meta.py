import datetime
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

from .causal_dependency import CausalDependency

__all__ = (
    "EventMeta",
    "IEventMetaExporter",
)


@dataclass(frozen=True)
class EventMeta:
    event_id: uuid.UUID | None = None
    causation_id: uuid.UUID | None = None
    correlation_id: uuid.UUID | None = None
    reason: str | None = None
    occurred_at: datetime.datetime | None = None
    causal_dependencies: tuple[CausalDependency] = ()

    def export(self, exporter: "IEventMetaExporter") -> None:
        exporter.set_event_id(self.event_id)
        exporter.set_causation_id(self.causation_id)
        exporter.set_correlation_id(self.correlation_id)
        exporter.set_reason(self.reason)
        exporter.set_occurred_at(self.occurred_at)
        for causal_dependency in self.causal_dependencies:
            exporter.add_causal_dependency(causal_dependency)


class IEventMetaExporter(metaclass=ABCMeta):

    @abstractmethod
    def set_event_id(self, value: uuid.UUID | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_causation_id(self, value: uuid.UUID | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_correlation_id(self, value: uuid.UUID | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_reason(self, value: str | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_occurred_at(self, value: datetime.datetime | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_causal_dependency(self, value: CausalDependency) -> None:
        raise NotImplementedError
