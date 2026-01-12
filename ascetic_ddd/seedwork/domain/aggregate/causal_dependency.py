import typing
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

__all__ = (
    "CausalDependency",
    "ICausalDependencyExporter",
)


@dataclass(frozen=True)
class CausalDependency:
    aggregate_id: typing.Any
    aggregate_type: str
    aggregate_version: int

    def export(self, exporter: "ICausalDependencyExporter") -> None:
        exporter.set_aggregate_id(self.aggregate_id)
        exporter.set_aggregate_type(self.aggregate_type)
        exporter.set_aggregate_version(self.aggregate_version)


class ICausalDependencyExporter(metaclass=ABCMeta):

    @abstractmethod
    def set_aggregate_id(self, value: typing.Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_aggregate_type(self, value: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_aggregate_version(self, value: int) -> None:
        raise NotImplementedError
