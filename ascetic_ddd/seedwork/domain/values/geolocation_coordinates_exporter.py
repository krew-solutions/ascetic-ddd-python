from decimal import Decimal

from .geolocation_coordinates import IGeolocationCoordinatesExporter

__all__ = ("GeolocationCoordinatesExporter",)


class GeolocationCoordinatesExporter(IGeolocationCoordinatesExporter):
    def __init__(self) -> None:
        self.data = {}

    def set_longitude(self, value: Decimal) -> None:
        self.data["longitude"] = value

    def set_latitude(self, value: Decimal) -> None:
        self.data["latitude"] = value

    def set_altitude(self, value: Decimal | None) -> None:
        self.data["altitude"] = value

    def set_accuracy(self, value: Decimal | None) -> None:
        self.data["accuracy"] = value

    def set_altitude_accuracy(self, value: Decimal | None) -> None:
        self.data["altitude_accuracy"] = value

    def set_heading(self, value: Decimal | None) -> None:
        self.data["heading"] = value

    def set_speed(self, value: Decimal | None) -> None:
        self.data["speed"] = value
