import datetime

from .time_range import ITimeRangeExporter

__all__ = ("TimeRangeExporter",)


class TimeRangeExporter(ITimeRangeExporter):
    def __init__(self) -> None:
        self.data = {}

    def set_lower(self, value: datetime.datetime) -> None:
        self.data["lower"] = value

    def set_upper(self, value: datetime.datetime) -> None:
        self.data["upper"] = value
