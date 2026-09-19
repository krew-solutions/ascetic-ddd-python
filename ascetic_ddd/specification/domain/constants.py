import operator
import typing
from enum import Enum

from ascetic_ddd.specification.domain import arithmetic, comparison

__all__ = ('OPERATOR', 'ASSOCIATIVITY', 'OPERATOR_MAPPING',)


class OPERATOR(str, Enum):
    """Supported operators."""

    # Comparison
    EQ = "="
    NE = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    LSHIFT = "<<"
    RSHIFT = ">>"
    IS = "IS"  # test for TRUE, FALSE, UNKNOWN, NULL
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    IN = "IN"
    NOT_IN = "NOT IN"
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"

    # Logical operators
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    # Mathematical
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"

    # Unary mathematical
    # The values are names, not spellings: ``"+"`` and ``"-"`` are the values
    # of ADD and SUB, and an Enum makes a member with a repeated value an
    # alias of the first, so ``OPERATOR.NEG is OPERATOR.SUB`` was true. How an
    # operator is spelled is for the notation that writes it to say.
    #
    # There is no unary plus, which was the other alias, of ADD: no node was
    # made of it - a lambda's ``+x`` is ``x`` - and an operator that nothing
    # writes is one more case in every reader of the tree.
    NEG = "-neg"

    # Orderable
    ASC = "ASC"
    DESC = "DESC"

    # Others
    PERIOD = "."


class ASSOCIATIVITY(str, Enum):
    """Operator associativity types."""

    LEFT_ASSOCIATIVE = "LEFT"
    RIGHT_ASSOCIATIVE = "RIGHT"
    NON_ASSOCIATIVE = "NON"


OPERATOR_MAPPING: dict[OPERATOR, typing.Callable[..., typing.Any]] = {
    # Comparisons are PostgreSQL's for the values PostgreSQL has, so that the
    # evaluator and the query of the same tree agree; see ``comparison``.
    OPERATOR.EQ: comparison.eq,
    OPERATOR.NE: comparison.ne,
    OPERATOR.GT: comparison.gt,
    OPERATOR.LT: comparison.lt,
    OPERATOR.GTE: comparison.ge,
    OPERATOR.LTE: comparison.le,
    OPERATOR.IS: comparison.eq,
    OPERATOR.IS_NULL: lambda operand: operand is None,
    OPERATOR.IS_NOT_NULL: lambda operand: operand is not None,

    OPERATOR.AND: operator.and_,
    OPERATOR.OR: operator.or_,
    OPERATOR.NOT: operator.not_,

    # Arithmetic is PostgreSQL's for the values PostgreSQL has, so that the
    # evaluator and the query of the same tree agree; see ``arithmetic``.
    OPERATOR.NEG: arithmetic.neg,

    OPERATOR.ADD: arithmetic.add,
    OPERATOR.SUB: arithmetic.sub,
    OPERATOR.MUL: arithmetic.mul,
    OPERATOR.DIV: arithmetic.div,
    OPERATOR.MOD: arithmetic.mod,
    OPERATOR.RSHIFT: arithmetic.rshift,
    OPERATOR.LSHIFT: arithmetic.lshift,
}
