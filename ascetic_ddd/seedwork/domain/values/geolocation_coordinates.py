"""See https://developer.mozilla.org/en-US/docs/Web/API/GeolocationCoordinates."""
from abc import ABCMeta, abstractmethod
from decimal import Decimal

try:
    from geopy.distance import geodesic
except ImportError:
    geodesic = None

from .point import Point

__all__ = (
    "GeolocationCoordinates",
    "IGeolocationCoordinatesExporter",
)


class GeolocationCoordinates:
    _latitude: Decimal
    _longitude: Decimal
    _altitude: Decimal | None = None
    _accuracy: Decimal | None = None
    _altitude_accuracy: Decimal | None = None
    _heading: Decimal | None = None
    _speed: Decimal | None = None

    def __init__(
        self,
        latitude: Decimal,
        longitude: Decimal,
        altitude: Decimal | None = None,
        accuracy: Decimal | None = None,
        altitude_accuracy: Decimal | None = None,
        heading: Decimal | None = None,
        speed: Decimal | None = None,
    ) -> None:
        self._latitude = latitude
        self._longitude = longitude
        self._altitude = altitude
        self._accuracy = accuracy
        self._altitude_accuracy = altitude_accuracy
        self._heading = heading
        self._speed = speed

    @property
    def point(self) -> Point:
        # Geopy cannot calculate distance with altitude
        return Point(self._latitude, self._longitude, None)

    def distance(self, location: Point):
        return geodesic(self.point, location).m - (float(self._accuracy) if self._accuracy else 0)

    def export(self, exporter: "IGeolocationCoordinatesExporter") -> None:
        exporter.set_latitude(self._latitude)
        exporter.set_longitude(self._longitude)
        exporter.set_altitude(self._altitude)
        exporter.set_accuracy(self._accuracy)
        exporter.set_altitude_accuracy(self._altitude_accuracy)
        exporter.set_heading(self._heading)
        exporter.set_speed(self._speed)


class IGeolocationCoordinatesExporter(metaclass=ABCMeta):

    @abstractmethod
    def set_longitude(self, value: Decimal) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_latitude(self, value: Decimal) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_altitude(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_accuracy(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_altitude_accuracy(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_heading(self, value: Decimal | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_speed(self, value: Decimal | None) -> None:
        raise NotImplementedError
