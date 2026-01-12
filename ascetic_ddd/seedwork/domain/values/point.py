from abc import ABCMeta, abstractmethod
from decimal import Decimal

try:
    from geopy import Point as _Point
except ImportError:

    class _Point:
        pass

__all__ = (
    "Point",
    "IPointExporter",
)


# TODO: Fix interface
class Point(_Point):
    def __new__(cls, latitude: Decimal, longitude: Decimal, altitude: Decimal | None = None):
        if latitude is None and longitude is None and altitude is None:
            return EmptyPoint()
        return super().__new__(cls, latitude, longitude, altitude)

    def export(self, exporter: "IPointExporter") -> None:
        exporter.longitude = Decimal(self.longitude).quantize(Decimal(".000001"))
        exporter.latitude = Decimal(self.latitude).quantize(Decimal(".000001"))
        exporter.altitude = (
            bool(self.altitude) and Decimal(self.altitude).quantize(Decimal(".000001")) or None
        )

    @staticmethod
    def empty() -> "EmptyPoint":
        return EmptyPoint()

    @property
    def is_empty(self) -> bool:
        return self == EmptyPoint()


class EmptyPoint(Point):
    def __new__(cls):
        return _Point.__new__(cls)

    def export(self, exporter: "IPointExporter") -> None:
        exporter.set_longitude(None)
        exporter.set_latitude(None)
        exporter.set_altitude(None)


class IPointExporter(metaclass=ABCMeta):

    @abstractmethod
    def set_longitude(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_latitude(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_altitude(self, value: Decimal | None) -> None:
        raise NotImplementedError
