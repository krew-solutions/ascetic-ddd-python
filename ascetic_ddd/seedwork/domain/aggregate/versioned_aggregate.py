import typing
from abc import ABCMeta, abstractmethod

from .interfaces import IVersionedAggregate

__all__ = (
    "VersionedAggregate",
    "IVersionedAggregateExporter",
    "IVersionedAggregateReconstitutor",
    "VersionedAggregateExporter",
    "VersionedAggregateReconstitutor",
)


class VersionedAggregate(IVersionedAggregate, metaclass=ABCMeta):
    __version: int

    def __init__(self, version: int = 0, **kwargs) -> None:
        self.__version = version
        super().__init__(**kwargs)

    @property
    def version(self) -> int:
        return self.__version

    @version.setter
    def version(self, value: int) -> None:
        self.__version = value

    def next_version(self) -> int:
        self.__version += 1
        return self.__version

    def export(self, exporter: "IVersionedAggregateExporter") -> None:
        exporter.set_version(self.version)

    def _import(self, provider: "IVersionedAggregateReconstitutor") -> None:
        self.version = provider.version()

    @classmethod
    def _make_empty(cls) -> typing.Self:
        agg = cls.__new__(cls)
        super(cls, agg).__init__()
        return agg

    @classmethod
    def reconstitute(cls, reconstitutor: "IVersionedAggregateReconstitutor") -> typing.Self:
        """
        For EventSourcedAggregate this method could be used to restore a snapshot.
        """
        agg: typing.Self = cls._make_empty()
        agg._import(reconstitutor)
        return agg


class IVersionedAggregateExporter(metaclass=ABCMeta):
    @abstractmethod
    def set_version(self, value: int) -> None:
        raise NotImplementedError


class IVersionedAggregateReconstitutor(typing.Protocol, metaclass=ABCMeta):
    @abstractmethod
    def version(self) -> int:
        raise NotImplementedError


class VersionedAggregateExporter(IVersionedAggregateExporter):
    def __init__(self):
        self.data = dict()

    def set_version(self, value: int) -> None:
        self.data['version'] = value


class VersionedAggregateReconstitutor(IVersionedAggregateReconstitutor):
    def __init__(self, version: int):
        self._data = locals()

    def version(self) -> int:
        return self._data['version']
