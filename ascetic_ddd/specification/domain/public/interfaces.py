import typing
import numbers

from ascetic_ddd.specification.domain.nodes import Visitable

__all__ = (
    'IDelegating',
    'INullable',
    'ILogical',
    'IComparison',
    'IMathematical',
)

T = typing.TypeVar("T")


class IDelegating(typing.Protocol):
    def delegate(self) -> Visitable:
        ...


class INullable(IDelegating, typing.Protocol):
    def is_null(self) -> 'ILogical':
        ...

    def is_not_null(self) -> 'ILogical':
        ...


class ILogical(IDelegating, typing.Protocol):
    def __and__(self, other: 'ILogical') -> 'ILogical':
        ...

    def __or__(self, other: 'ILogical') -> 'ILogical':
        ...

    def __invert__(self) -> 'ILogical':
        ...

    def is_(self, other: 'ILogical') -> 'ILogical':
        ...


class IComparison(IDelegating, typing.Protocol):
    def __eq__(self, other: 'IComparison') -> 'ILogical':  # type: ignore[override]
        ...

    def __ne__(self, other: 'IComparison') -> 'ILogical':  # type: ignore[override]
        ...

    def __gt__(self, other: 'IComparison') -> 'ILogical':
        ...

    def __lt__(self, other: 'IComparison') -> 'ILogical':
        ...

    def __ge__(self, other: 'IComparison') -> 'ILogical':
        ...

    def __le__(self, other: 'IComparison') -> 'ILogical':
        ...


O = typing.TypeVar('O', bound='IMathematical')


class IMathematical(IDelegating, typing.Protocol[T]):
    def __add__(self, other: O) -> O:
        ...

    def __sub__(self, other: 'IMathematical[T]') -> typing.Self:
        ...

    def __mul__(self, other: 'IMathematical[numbers.Number]') -> typing.Self:
        ...

    # `/` is `__truediv__`: it used to be `__div__`, which is Python 2's, so
    # `a / b` raised TypeError.
    def __truediv__(self, other: 'IMathematical[numbers.Number]') -> typing.Self:
        ...

    def __mod__(self, other: 'IMathematical[numbers.Number]') -> typing.Self:
        ...

    # `<<` and `>>` shift the bits of an integer, as the evaluator and
    # PostgreSQL have them: a computation. They used to be of IComparison,
    # with a truth value for a result.
    def __lshift__(self, other: 'IMathematical[numbers.Number]') -> typing.Self:
        ...

    def __rshift__(self, other: 'IMathematical[numbers.Number]') -> typing.Self:
        ...
