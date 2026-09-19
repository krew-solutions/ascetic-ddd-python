"""Arithmetic as PostgreSQL does it, for the values it shares with Python.

A specification has two readers that must agree: the evaluator, and the
database reading the query the same tree compiles to. Python's own operators
disagree with PostgreSQL's where they meet: ``7 / 2`` is 3.5 and not 3,
``-7 % 2`` is 1 and not -1, an integer has no bounds, ``1 << 64`` is a number
and not 1, a float overflows to ``inf`` and not to an error, ``True + 1`` is
2 and ``"a" * 3`` a string, where PostgreSQL has no such operators.

The functions here are PostgreSQL's operators for ``int`` (``bigint``),
``float`` (``double precision``), ``bool`` and ``str``. Anything else - a
Value Object, a ``Decimal``, a ``datetime`` - computes as its type defines,
which is what it is for: a timestamp less a timestamp is an interval in both
worlds already.
"""
import math
import operator
import typing

__all__ = (
    'add', 'sub', 'mul', 'div', 'mod', 'lshift', 'rshift', 'neg',
)

BIGINT_MIN = -2 ** 63
BIGINT_MAX = 2 ** 63 - 1

_Binary = typing.Callable[[typing.Any, typing.Any], typing.Any]
_Unary = typing.Callable[[typing.Any], typing.Any]


def _is_integer(value: typing.Any) -> bool:
    # Not isinstance: bool is a subclass of int, and is not a bigint.
    return type(value) is int


def _is_number(value: typing.Any) -> bool:
    return type(value) is int or type(value) is float


def _has_no_arithmetic(value: typing.Any) -> bool:
    return type(value) is bool or type(value) is str


def _bigint(value: int) -> int:
    """Return ``value``, which must fit a bigint."""
    if not BIGINT_MIN <= value <= BIGINT_MAX:
        raise OverflowError("bigint out of range")
    return value


def _wrapped(value: int) -> int:
    """Return the bigint the low 64 bits of ``value`` are: what a shift leaves."""
    return (value - BIGINT_MIN) % 2 ** 64 + BIGINT_MIN


def _double(value: float, left: float, right: float) -> float:
    """Return ``value``, which must not be an overflow of finite operands."""
    if math.isinf(value) and not (math.isinf(left) or math.isinf(right)):
        raise OverflowError("value out of range: overflow")
    return value


def _truncated(left: int, right: int) -> int:
    """Return the quotient rounded towards zero; Python's ``//`` rounds down."""
    quotient = abs(left) // abs(right)
    return -quotient if (left < 0) != (right < 0) else quotient


def _remainder(left: int, right: int) -> int:
    """Return the remainder with the sign of the dividend; Python's ``%``
    has the sign of the divisor."""
    return left - right * _truncated(left, right)


def _unsupported(symbol: str, *operands: typing.Any) -> TypeError:
    return TypeError(
        'operator "%s" is not supported for %s'
        % (symbol, " and ".join(type(operand).__name__ for operand in operands))
    )


def _binary(
    symbol: str,
    of_integers: _Binary,
    of_floats: _Binary | None,
    otherwise: _Binary,
) -> _Binary:
    """Make the operator ``symbol``.

    Args:
        symbol: How the operator is written, for an error to show
        of_integers: What it is of two integers, before the check of bounds
        of_floats: What it is of two numbers one of which is a float; None if
            PostgreSQL has no such operator
        otherwise: What it is of values that are not PostgreSQL's scalars

    Returns:
        The operator, as a function of its two operands
    """
    def compute(left: typing.Any, right: typing.Any) -> typing.Any:
        if _is_integer(left) and _is_integer(right):
            return of_integers(left, right)
        if _is_number(left) and _is_number(right):
            if of_floats is None:
                raise _unsupported(symbol, left, right)
            return _double(of_floats(left, right), left, right)
        if _has_no_arithmetic(left) or _has_no_arithmetic(right):
            raise _unsupported(symbol, left, right)
        return otherwise(left, right)

    return compute


def _unary(symbol: str, of_integer: _Unary, otherwise: _Unary) -> _Unary:
    """Make the unary operator ``symbol``; see ``_binary``."""
    def compute(operand: typing.Any) -> typing.Any:
        if _is_integer(operand):
            return of_integer(operand)
        if _has_no_arithmetic(operand):
            raise _unsupported(symbol, operand)
        return otherwise(operand)

    return compute


add = _binary("+", lambda l, r: _bigint(l + r), operator.add, operator.add)
sub = _binary("-", lambda l, r: _bigint(l - r), operator.sub, operator.sub)
mul = _binary("*", lambda l, r: _bigint(l * r), operator.mul, operator.mul)
div = _binary("/", lambda l, r: _bigint(_truncated(l, r)), operator.truediv, operator.truediv)
mod = _binary("%", _remainder, None, operator.mod)
# PostgreSQL takes the count of a shift modulo 64, a negative count included,
# and drops the bits shifted out.
lshift = _binary("<<", lambda l, r: _wrapped(l << (r & 63)), None, operator.lshift)
rshift = _binary(">>", lambda l, r: l >> (r & 63), None, operator.rshift)
neg = _unary("-", lambda v: _bigint(-v), operator.neg)
