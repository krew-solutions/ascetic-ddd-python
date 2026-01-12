import typing
from abc import ABCMeta

from .interfaces import IEventiveEntity

__all__ = ("EventiveEntity",)

IDE = typing.TypeVar("IDE", covariant=True)


class EventiveEntity(typing.Generic[IDE], IEventiveEntity[IDE], metaclass=ABCMeta):
    def __init__(self, **kwargs) -> None:
        self.__pending_domain_events = []
        super().__init__(**kwargs)

    def _add_domain_event(self, event: IDE) -> None:
        self.__pending_domain_events.append(event)

    @property
    def pending_domain_events(self) -> typing.Iterable[IDE]:
        return tuple(self.__pending_domain_events)

    @pending_domain_events.deleter
    def pending_domain_events(self) -> None:
        self.__pending_domain_events.clear()
