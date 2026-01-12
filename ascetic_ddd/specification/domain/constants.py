import operator
from enum import Enum

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
    IS_NOT_NULL = "IS NULL"
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
    POS = "+"
    NEG = "-"

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


OPERATOR_MAPPING = {
    OPERATOR.EQ: operator.eq,
    OPERATOR.NE: operator.ne,
    OPERATOR.GT: operator.gt,
    OPERATOR.LT: operator.lt,
    OPERATOR.GTE: operator.ge,
    OPERATOR.LTE: operator.le,
    OPERATOR.IS: operator.eq,
    OPERATOR.IS_NULL: lambda operand: operand is None,
    OPERATOR.IS_NOT_NULL: lambda operand: operand is not None,

    OPERATOR.AND: operator.and_,
    OPERATOR.OR: operator.or_,
    OPERATOR.NOT: operator.not_,

    OPERATOR.ADD: operator.add,
    OPERATOR.SUB: operator.sub,
    OPERATOR.MUL: operator.mul,
    OPERATOR.DIV: operator.truediv,
    OPERATOR.MOD: operator.mod,
    OPERATOR.RSHIFT: operator.rshift,
    OPERATOR.LSHIFT: operator.lshift,
}
