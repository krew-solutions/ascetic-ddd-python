import typing

from ascetic_ddd.observable.interfaces import IObservable


__all__ = ('IPgRepository',)


class IPgRepository(IObservable, typing.Protocol):
    @property
    def table(self) -> str:
        ...
