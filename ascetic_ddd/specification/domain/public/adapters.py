"""
See also: https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/sql/operators.py#L101
"""
import numbers
import typing

from ascetic_ddd.specification.domain.public.interfaces import INullable, ILogical, IDelegating, IComparison, IMathematical
from ascetic_ddd.specification.domain import nodes

__all__ = (
    'Delegating',
    'Factory',
    'Logical',
    'Nullable',
    'Comparison',
    'Mathematical',
    'object_',
    'field',
)

T = typing.TypeVar("T")
FactoryT = typing.TypeVar("FactoryT", bound="Factory")


class Delegating(IDelegating):
    _delegate: nodes.Visitable

    def __init__(self, delegate: nodes.Visitable):
        self._delegate = delegate

    def delegate(self) -> nodes.Visitable:
        return self._delegate


class Factory(typing.Generic[T]):
    @classmethod
    def make_field(cls: type[FactoryT], name: str) -> FactoryT:
        return cls(field(name))  # type: ignore[call-arg]

    @classmethod
    def make_value(cls: type[FactoryT], value: T) -> FactoryT:
        return cls(nodes.Value(value))  # type: ignore[call-arg]


class Logical(Delegating, ILogical):

    def __and__(self, other: ILogical) -> ILogical:
        return Logical(nodes.And(self.delegate(), other.delegate()))

    def __or__(self, other: ILogical) -> ILogical:
        return Logical(nodes.Or(self.delegate(), other.delegate()))

    def __invert__(self) -> ILogical:
        return Logical(nodes.Not(self.delegate()))

    def is_(self, other: ILogical) -> ILogical:
        return Logical(nodes.Is(self.delegate(), other.delegate()))


class Nullable(Delegating, INullable):
    def is_null(self) -> ILogical:
        return Logical(nodes.IsNull(self.delegate()))

    def is_not_null(self) -> ILogical:
        return Logical(nodes.IsNotNull(self.delegate()))


class Comparison(Delegating, IComparison):
    # Equality with the null constant is the null test, as in a template and
    # in a lambda: see nodes.equality_or_null_test.
    def __eq__(self, other: IComparison) -> ILogical:  # type: ignore[override]
        return Logical(nodes.equality_or_null_test(nodes.Equal, self.delegate(), other.delegate()))

    def __ne__(self, other: IComparison) -> ILogical:  # type: ignore[override]
        return Logical(nodes.equality_or_null_test(nodes.NotEqual, self.delegate(), other.delegate()))

    def __gt__(self, other: IComparison) -> ILogical:
        return Logical(nodes.GreaterThan(self.delegate(), other.delegate()))

    def __lt__(self, other: IComparison) -> ILogical:
        return Logical(nodes.LessThan(self.delegate(), other.delegate()))

    def __ge__(self, other: IComparison) -> ILogical:
        return Logical(nodes.GreaterThanEqual(self.delegate(), other.delegate()))

    def __le__(self, other: IComparison) -> ILogical:
        return Logical(nodes.LessThanEqual(self.delegate(), other.delegate()))


O = typing.TypeVar('O', bound=IMathematical)


class Mathematical(Delegating, IMathematical[T], typing.Generic[T]):
    def __add__(self, other: O) -> O:
        return typing.cast(O, type(self)(nodes.Add(self.delegate(), other.delegate())))

    def __sub__(self, other: IMathematical[T]) -> typing.Self:
        return type(self)(nodes.Sub(self.delegate(), other.delegate()))

    def __mul__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Mul(self.delegate(), other.delegate()))

    def __truediv__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Div(self.delegate(), other.delegate()))

    def __mod__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Mod(self.delegate(), other.delegate()))

    def __lshift__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.LeftShift(self.delegate(), other.delegate()))

    def __rshift__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.RightShift(self.delegate(), other.delegate()))


def object_(name: str) -> nodes.Object:
    parent: typing.Union[nodes.GlobalScope, nodes.Object] = nodes.GlobalScope()
    parts = name.split(".")
    while parts:
        parent = nodes.Object(parent, parts.pop(0))
    assert isinstance(parent, nodes.Object)
    return parent


def field(name: str) -> nodes.Field:
    idx = name.rfind(".")
    if idx != -1:
        return nodes.Field(object_(name[:idx]), name[idx + 1:])
    return nodes.Field(nodes.GlobalScope(), name)
