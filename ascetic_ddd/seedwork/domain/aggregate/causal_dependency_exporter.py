import typing

from .causal_dependency import ICausalDependencyExporter

__all__ = ("CausalDependencyExporter",)


class CausalDependencyExporter(ICausalDependencyExporter):
    def __init__(self) -> None:
        self.data = {}

    def set_aggregate_id(self, value: typing.Any) -> None:
        self.data["aggregate_id"] = value

    def set_aggregate_type(self, value: str) -> None:
        self.data["aggregate_type"] = value

    def set_aggregate_version(self, value: int) -> None:
        self.data["aggregate_version"] = value
