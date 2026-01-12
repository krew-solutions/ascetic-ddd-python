import typing
from datetime import datetime

from .adapters import Factory, Logical, Comparison, Mathematical, Nullable

__all__ = (
    'Boolean',
    'NullBoolean',
    'Number',
    'NullNumber',
    'Datetime',
    'NullDatetime',
    'Text',
    'NullText',
)

T = typing.TypeVar("T")


class Boolean(Logical, Factory[bool]):
    pass


class NullBoolean(Boolean, Nullable):
    pass


class Number(Comparison, Mathematical[T], Factory[T], typing.Generic[T]):
    pass


class NullNumber(Number[T], Nullable, typing.Generic[T]):
    pass


class Datetime(Comparison, Mathematical[datetime], Factory[datetime]):
    pass


class NullDatetime(Datetime, Nullable):
    pass


class Text(Comparison, Factory[T], typing.Generic[T]):
    pass


class NullText(Text[T], Nullable, typing.Generic[T]):
    pass
