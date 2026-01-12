import datetime
from abc import ABCMeta, abstractmethod

from psycopg.types.range import TimestamptzRange

__all__ = (
    "TimeRange",
    "ITimeRangeExporter",
)


# TODO: Fix interface
class TimeRange(TimestamptzRange):
    def __init__(
        self,
        lower: datetime.datetime | None = None,
        upper: datetime.datetime | None = None,
    ):
        if lower and not isinstance(lower, datetime.datetime):
            raise ValueError(
                "Type of Timeslot.lower should be datetime.datetime, not %r", (lower,)
            )

        if upper and not isinstance(upper, datetime.datetime):
            raise ValueError(
                "Type of Timeslot.upper should be datetime.datetime, not %r", (upper,)
            )

        if lower and upper and lower > upper:
            raise ValueError(
                "Range lower %s bound must be less than or equal to range upper %s bound",
                (lower, upper),
            )
        super().__init__(lower, upper)

    def export(self, exporter: "ITimeRangeExporter") -> None:
        exporter.set_lower(self.lower)
        exporter.set_upper(self.upper)


class ITimeRangeExporter(metaclass=ABCMeta):

    @abstractmethod
    def set_lower(self, value: datetime.datetime) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_upper(self, value: datetime.datetime) -> None:
        raise NotImplementedError
