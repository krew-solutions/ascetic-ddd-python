import typing

from ascetic_ddd.observable.interfaces import IObservable


__all__ = ('IPgExternalSource',)


@typing.runtime_checkable
class IPgExternalSource(IObservable, typing.Protocol):
    @property
    def table(self) -> str:
        ...
