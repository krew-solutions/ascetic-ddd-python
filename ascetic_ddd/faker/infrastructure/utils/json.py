"""Copied from https://github.com/django/django/blob/main/django/core/serializers/json.py."""
import datetime
import dataclasses
import decimal
import json
import uuid

from ...domain.values.json import Json

__all__ = ("JSONEncoder",)


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime.datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith("+00:00"):
                r = r.removesuffix("+00:00") + "Z"
            return r
        if isinstance(o, datetime.date):
            return o.isoformat()
        if isinstance(o, datetime.time):
            if o.utcoffset() is not None:
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        if isinstance(o, datetime.timedelta):
            return duration_iso_string(o)
        if isinstance(o, decimal.Decimal | uuid.UUID):
            return str(o)
        if isinstance(o, Json):
            return o.obj
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)

        return super().default(o)


def duration_iso_string(duration):
    if duration < datetime.timedelta(0):
        sign = "-"
        duration *= -1
    else:
        sign = ""

    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)
    ms = f".{microseconds:06d}" if microseconds else ""
    return f"{sign}P{days}DT{hours:02d}H{minutes:02d}M{seconds:02d}{ms}S"


def _get_duration_components(duration):
    days = duration.days
    seconds = duration.seconds
    microseconds = duration.microseconds

    minutes = seconds // 60
    seconds %= 60

    hours = minutes // 60
    minutes %= 60

    return days, hours, minutes, seconds, microseconds


def duration_string(duration):
    """Version of str(timedelta) which is not English specific."""
    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)

    string = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if days:
        string = f"{days} " + string
    if microseconds:
        string += f".{microseconds:06d}"

    return string
