import typing

from ..aggregate import IHashable
from ....specification.domain.interfaces import IEqualOperand
from .interfaces import IAccessible

__all__ = ("Identity",)


T = typing.TypeVar("T")


class Identity(typing.Generic[T], IAccessible[T], IHashable):
    def __init__(self, value: T | None):
        self._value = value

    @property
    def value(self) -> T:
        return self._value

    def is_transient(self) -> bool:
        return self._value is None

    @classmethod
    def transient(cls):
        return cls(None)

    def __hash__(self) -> int:
        return hash(self._value)

    def __eq__(self, other: IEqualOperand) -> bool:
        assert isinstance(other, Identity)
        return self._value == other._value

    def __str__(self) -> str:
        return str(self._value)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._value!r})"

    def export(self, setter: typing.Callable[[T], None]) -> None:
        setter(self._value)

    def import_(self, value: T):
        if self.is_transient():
            raise TypeError("identity should be transient")
        self._value = value
