from decimal import Decimal

from .point import IPointExporter

__all__ = ("PointExporter",)


class PointExporter(IPointExporter):
    def __init__(self) -> None:
        self.data = {}

    def set_longitude(self, value: Decimal | None) -> None:
        self.data["longitude"] = value

    def set_latitude(self, value: Decimal | None) -> None:
        self.data["latitude"] = value

    def set_altitude(self, value: Decimal | None) -> None:
        self.data["altitude"] = value
