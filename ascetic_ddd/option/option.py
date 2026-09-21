import typing

T = typing.TypeVar("T")
U = typing.TypeVar("U")

__all__ = ("Option", "Some", "Nothing")


class Option(typing.Generic[T]):
    """Optional value: every Option is either Some (contains a value) or Nothing (does not)."""

    __slots__ = ("_val", "_valid")

    def __init__(self, val: T, valid: bool):
        self._val = val
        self._valid = valid

    def is_some(self) -> bool:
        return self._valid

    def is_nothing(self) -> bool:
        return not self._valid

    def is_some_and(self, f: typing.Callable[[T], bool]) -> bool:
        """Returns True if the Option is Some and the contained value satisfies the predicate."""
        return self._valid and f(self._val)

    def is_nothing_or(self, f: typing.Callable[[T], bool]) -> bool:
        """Returns True if the Option is Nothing, or the contained value satisfies the predicate."""
        return not self._valid or f(self._val)

    def unwrap(self) -> T:
        """Returns the contained value.

        Raises:
            ValueError: If the Option is Nothing.
        """
        if not self._valid:
            raise ValueError("called unwrap on a Nothing Option")
        return self._val

    def unwrap_or(self, default: T) -> T:
        """Returns the contained value or the provided default."""
        if self._valid:
            return self._val
        return default

    def unwrap_or_else(self, f: typing.Callable[[], T]) -> T:
        """Returns the contained value or computes it from the callable."""
        if self._valid:
            return self._val
        return f()

    def map(self, f: typing.Callable[[T], U]) -> "Option[U]":
        """Applies a function to the contained value (if Some), or returns Nothing."""
        if self._valid:
            return Some(f(self._val))
        return Nothing()

    def map_or(self, default: U, f: typing.Callable[[T], U]) -> U:
        """Applies a function to the contained value (if Some), or returns the default."""
        if self._valid:
            return f(self._val)
        return default

    def and_then(self, f: typing.Callable[[T], "Option[U]"]) -> "Option[U]":
        """Returns Nothing if Nothing, otherwise calls f with the contained value."""
        if self._valid:
            return f(self._val)
        return Nothing()

    def __or__(self, optb: "Option[T]") -> "Option[T]":
        """Returns self if Some, otherwise returns optb."""
        if self._valid:
            return self
        return optb

    def or_else(self, f: typing.Callable[[], "Option[T]"]) -> "Option[T]":
        """Returns self if Some, otherwise calls f."""
        if self._valid:
            return self
        return f()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Option):
            return NotImplemented
        if self._valid != other._valid:
            return False
        if not self._valid:
            return True
        return self._val == other._val

    def __hash__(self) -> int:
        if not self._valid:
            return hash((False, None))
        return hash((True, self._val))

    def __repr__(self) -> str:
        if self._valid:
            return "Some(%r)" % (self._val,)
        return "Nothing"

    def __str__(self) -> str:
        if self._valid:
            return "Some(%s)" % (self._val,)
        return "Nothing"


def Some(val: T) -> Option[T]:
    """Creates an Option containing the given value."""
    return Option(val, True)


def Nothing() -> Option[typing.Any]:
    """Creates an empty Option."""
    return Option(None, False)
