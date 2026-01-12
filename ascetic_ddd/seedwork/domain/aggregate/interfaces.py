import typing
from abc import ABCMeta, abstractmethod

from ....specification.domain.interfaces import IEqualOperand

__all__ = (
    "IVersionedAggregate",
    "IDomainEventAdder",
    "IDomainEventAccessor",
    "IEventiveEntity",
    "IDomainEventLoader",
    "IEventSourcedAggregate",
    "IHashable",
)


class IVersionedAggregate(metaclass=ABCMeta):
    @property
    @abstractmethod
    def version(self) -> int:
        raise NotImplementedError

    @version.setter
    @abstractmethod
    def version(self, value: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def next_version(self) -> int:
        raise NotImplementedError


IDE = typing.TypeVar("IDE", covariant=True)


class IDomainEventAdder(typing.Generic[IDE], metaclass=ABCMeta):
    @abstractmethod
    def _add_domain_event(self, event: IDE):
        raise NotImplementedError


class IDomainEventAccessor(typing.Generic[IDE], metaclass=ABCMeta):
    @property
    @abstractmethod
    def pending_domain_events(self) -> typing.Iterable[IDE]:
        raise NotImplementedError

    @pending_domain_events.deleter
    @abstractmethod
    def pending_domain_events(self) -> None:
        raise NotImplementedError


class IEventiveEntity(
    typing.Generic[IDE], IDomainEventAdder[IDE], IDomainEventAccessor[IDE], metaclass=ABCMeta
):
    pass


IPDE = typing.TypeVar("IPDE", covariant=True)


class IDomainEventLoader(typing.Generic[IPDE], metaclass=ABCMeta):
    @abstractmethod
    def _load_from(self, past_events: typing.Iterable[IPDE]) -> None:
        raise NotImplementedError


class IEventSourcedAggregate(
    typing.Generic[IPDE],
    IDomainEventLoader[IDomainEventLoader],
    IEventiveEntity[IPDE],
    IVersionedAggregate,
    metaclass=ABCMeta,
):
    @abstractmethod
    def _update(self, e: IPDE) -> None:
        raise NotImplementedError


class IHashable(IEqualOperand, typing.Protocol, metaclass=ABCMeta):
    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError
