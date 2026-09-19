"""Comparisons as PostgreSQL has them, for the values it shares with Python.

The counterpart of ``arithmetic``, and for the same reason: the evaluator and
the database reading the query of the same tree must agree. Python compares
what PostgreSQL has no operator for - ``"a" == 1`` is False, ``True == 1`` is
True - so a specification that compares a text with a number was wrong for
one of its two readers only. And it orders NaN as IEEE 754 does, equal to
nothing, where PostgreSQL has it equal to itself and greater than any number,
so that a float can be sorted and indexed.

The functions here are PostgreSQL's comparisons for ``bool``, ``int`` and
``float`` (one kind, numbers) and ``str``. Anything else - a Value Object, a
``Decimal``, a ``datetime`` - compares as its type defines.
"""
import math
import operator
import typing

__all__ = ('eq', 'ne', 'gt', 'lt', 'ge', 'le')

_Comparison = typing.Callable[[typing.Any, typing.Any], typing.Any]


def _kind(value: typing.Any) -> str | None:
    """Return the kind of a PostgreSQL scalar; None for a value of another type."""
    # Not isinstance: bool is a subclass of int, and is not a number.
    if type(value) is bool:
        return "boolean"
    if type(value) is int or type(value) is float:
        return "number"
    if type(value) is str:
        return "text"
    return None


def _is_nan(value: typing.Any) -> bool:
    return type(value) is float and math.isnan(value)


def _comparison(
    symbol: str,
    of_values: _Comparison,
    of_order: typing.Callable[[int], bool],
) -> _Comparison:
    """Make the comparison ``symbol``.

    Args:
        symbol: How the operator is written, for an error to show
        of_values: What it is of two values, as Python has it
        of_order: What it is of the order of two numbers one of which is NaN:
            negative if the left is less, zero if they are equal

    Returns:
        The comparison, as a function of its two operands
    """
    def compare(left: typing.Any, right: typing.Any) -> typing.Any:
        left_kind, right_kind = _kind(left), _kind(right)
        if left_kind is not None and right_kind is not None and left_kind != right_kind:
            raise TypeError(
                'operator "%s" is not supported for %s and %s'
                % (symbol, type(left).__name__, type(right).__name__)
            )
        if _is_nan(left) or _is_nan(right):
            # NaN equals itself and is greater than any other number.
            return of_order(int(_is_nan(left)) - int(_is_nan(right)))
        return of_values(left, right)

    return compare


eq = _comparison("=", operator.eq, lambda order: order == 0)
ne = _comparison("!=", operator.ne, lambda order: order != 0)
gt = _comparison(">", operator.gt, lambda order: order > 0)
lt = _comparison("<", operator.lt, lambda order: order < 0)
ge = _comparison(">=", operator.ge, lambda order: order >= 0)
le = _comparison("<=", operator.le, lambda order: order <= 0)
