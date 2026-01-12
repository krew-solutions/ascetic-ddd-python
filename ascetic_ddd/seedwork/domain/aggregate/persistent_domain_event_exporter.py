from .event_meta import EventMeta
from .event_meta_exporter import EventMetaExporter
from .persistent_domain_event import IPersistentDomainEventExporter

__all__ = ("PersistentDomainEventExporter",)


class PersistentDomainEventExporter(IPersistentDomainEventExporter):
    def __init__(self) -> None:
        self.data = {}

    def set_event_type(self, value: str) -> None:
        self.data["event_type"] = value

    def set_event_version(self, value: int) -> None:
        self.data["event_version"] = value

    def set_event_meta(self, meta: EventMeta) -> None:
        exporter = EventMetaExporter()
        meta.export(exporter)
        self.data["event_meta"] = exporter.data

    def set_aggregate_version(self, value: int) -> None:
        self.data["aggregate_version"] = value
