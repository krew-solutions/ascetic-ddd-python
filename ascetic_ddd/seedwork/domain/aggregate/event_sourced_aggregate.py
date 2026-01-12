import dataclasses
import typing
from abc import ABCMeta

from . import PersistentDomainEvent
from .eventive_entity import EventiveEntity
from .interfaces import IEventSourcedAggregate
from .versioned_aggregate import VersionedAggregate

__all__ = ("EventSourcedAggregate",)

IPDE = typing.TypeVar("IPDE", covariant=True)


class EventSourcedAggregate(
    typing.Generic[IPDE],
    EventiveEntity[IPDE],
    VersionedAggregate,
    IEventSourcedAggregate[IPDE],
    metaclass=ABCMeta,
):
    class Handlers(dict):
        def register(self, event_type: type[IPDE]):
            def do_register(handler: typing.Callable[["EventSourcedAggregate", IPDE], None]):
                self[event_type] = handler
                return handler

            return do_register

    _handlers = Handlers()

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def _load_from(self, past_events: typing.Iterable[IPDE]) -> None:
        for event in past_events:
            self.version = event.aggregate_version
            self._handlers[type(event)](self, event)

    def _update(self, event: IPDE) -> None:
        event = dataclasses.replace(event, aggregate_version=self.next_version())
        self._add_domain_event(event)
        self._handlers[type(event)](self, event)

    @classmethod
    def fold(cls, past_events: typing.Iterable[PersistentDomainEvent]):
        """
        Or reduce.
        """
        agg: typing.Self = cls.make_empty()
        agg._load_from(past_events)
        return agg
