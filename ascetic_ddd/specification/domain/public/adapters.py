"""
See also: https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/sql/operators.py#L101
"""
import numbers
import typing

from .interfaces import INullable, ILogical, IDelegating, IComparison, IMathematical
from ..import nodes

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
T_Factory = typing.TypeVar("T_Factory", bound="Factory")


class Delegating(IDelegating):
    _delegate: nodes.Visitable

    def __init__(self, delegate: nodes.Visitable):
        self._delegate = delegate

    def delegate(self) -> nodes.Visitable:
        return self._delegate


class Factory(typing.Generic[T]):
    @classmethod
    def make_field(cls: type[T_Factory], name: str) -> T_Factory:
        return cls(field(name))

    @classmethod
    def make_value(cls: type[T_Factory], value: T) -> T_Factory:
        return cls(nodes.Value(value))


class Logical(Delegating, ILogical):

    def __and__(self, other: ILogical) -> ILogical:
        return Logical(nodes.And(self.delegate(), other.delegate()))

    def __or__(self, other: ILogical) -> ILogical:
        return Logical(nodes.Or(self.delegate(), other.delegate()))

    def is_(self, other: ILogical) -> ILogical:
        return Logical(nodes.Is(self.delegate(), other.delegate()))


class Nullable(Delegating, INullable):
    def is_null(self) -> ILogical:
        return Logical(nodes.IsNull(self.delegate()))

    def is_not_null(self) -> ILogical:
        return Logical(nodes.IsNotNull(self.delegate()))


class Comparison(Delegating, IComparison):
    def __eq__(self, other: IComparison) -> ILogical:
        return Logical(nodes.Equal(self.delegate(), other.delegate()))

    def __ne__(self, other: IComparison) -> ILogical:
        return Logical(nodes.NotEqual(self.delegate(), other.delegate()))

    def __gt__(self, other: IComparison) -> ILogical:
        return Logical(nodes.GreaterThan(self.delegate(), other.delegate()))

    def __lt__(self, other: IComparison) -> ILogical:
        return Logical(nodes.LessThan(self.delegate(), other.delegate()))

    def __ge__(self, other: IComparison) -> ILogical:
        return Logical(nodes.GreaterThanEqual(self.delegate(), other.delegate()))

    def __le__(self, other: IComparison) -> ILogical:
        return Logical(nodes.LessThanEqual(self.delegate(), other.delegate()))

    def __lshift__(self, other: IComparison) -> ILogical:
        return Logical(nodes.LeftShift(self.delegate(), other.delegate()))

    def __rshift__(self, other: IComparison) -> ILogical:
        return Logical(nodes.RightShift(self.delegate(), other.delegate()))


O = typing.TypeVar('O', bound=IMathematical)


class Mathematical(Delegating, IMathematical[T], typing.Generic[T]):
    def __add__(self, other: O) -> O:
        return typing.cast(O, type(self)(nodes.Add(self.delegate(), other.delegate())))

    def __sub__(self, other: IMathematical[T]) -> typing.Self:
        return type(self)(nodes.Sub(self.delegate(), other.delegate()))

    def __mul__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Mul(self.delegate(), other.delegate()))

    def __div__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Div(self.delegate(), other.delegate()))

    def __mod__(self, other: IMathematical[numbers.Number]) -> typing.Self:
        return type(self)(nodes.Mod(self.delegate(), other.delegate()))


def object_(name: str) -> nodes.Object:
    parent: typing.Union[nodes.GlobalScope, nodes.Object] = nodes.GlobalScope()
    parts = name.split(".")
    while parts:
        parent = nodes.Object(parent, parts.pop(0))
    return parent


def field(name: str) -> nodes.Field:
    idx = name.rfind(".")
    if idx != -1:
        return nodes.Field(object_(name[:idx]), name[idx + 1:])
    return nodes.Field(nodes.GlobalScope(), name)
