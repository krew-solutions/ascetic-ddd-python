"""Composite expression for handling composite keys.

Not a node of the tree, though the module keeps its name: what a transform
context may return instead of a node - several expressions, compared part
by part - and what the transformer turns into nodes.
"""
import typing
from typing import Protocol

from ascetic_ddd.specification.domain.nodes import (
    Visitable,
    And,
    Equal,
    Not,
    equality_or_null_test,
)

__all__ = (
    'CompositeExpression',
    'CompositeExpressionIsEmptyError',
    'CompositeExpressionsDifferentLengthError',
    'ICompositeExpression',
    'Mapped',
)



class CompositeExpressionsDifferentLengthError(Exception):
    """Raised when composite expressions have different lengths."""

    pass


class CompositeExpressionIsEmptyError(Exception):
    """Raised when a composite expression has no parts: it stands for nothing
    to compare. It used to be an IndexError from inside the comparison."""

    pass


class ICompositeExpression(Protocol):
    """Interface for expression composers."""

    def __eq__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """Create equality expression with another composite."""
        ...

    def __ne__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """Create not-equal expression with another composite."""
        ...


class CompositeExpression:
    """Several expressions that stand for one value of the domain (e.g.,
    composite key), a part of which may be composite itself.

    It used to be a Visitable whose ``accept`` raised NotImplementedError: a
    node of a class no visitor knew, which the transformer had to catch
    before anything visited it. It is not a node. It is what a mapping may
    return instead of one, ``Mapped``, and the transformer's result has no
    such case: one that is left over - under ``<``, under ``NOT``, as the
    whole specification - is the transformer's error, and nothing after it
    can meet one.
    """

    def __init__(self, *nodes: "Mapped"):
        self._nodes = list(nodes)

    def nodes(self) -> list["Mapped"]:
        """Return the expressions the composite stands for, in order."""
        return list(self._nodes)

    def __eq__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """
        Create an AND expression of equality comparisons.

        For composite keys: (a1 = b1) AND (a2 = b2) AND ...
        """
        if len(self._nodes) != len(other._nodes):
            raise CompositeExpressionsDifferentLengthError(
                "Composite expressions have different length"
            )
        if not self._nodes:
            raise CompositeExpressionIsEmptyError("A composite expression has no parts")

        operands = []
        for i in range(len(self._nodes)):
            left, right = self._nodes[i], other._nodes[i]

            # A part that is composite is compared with a composite part. One
            # on the right alone used to be compared as if it were a node.
            if isinstance(left, CompositeExpression) or isinstance(right, CompositeExpression):
                if not isinstance(left, CompositeExpression) or not isinstance(right, CompositeExpression):
                    raise CompositeExpressionsDifferentLengthError(
                        "Composite expressions have different length"
                    )
                new_node = left == right
                operands.append(new_node)
            else:
                # A part the mapping made the storage's null is tested for, as
                # a whole is: `b = $1` with a null is true of nothing.
                operands.append(equality_or_null_test(Equal, left, right))

        # A composite of one part is that part: And takes two operands and
        # more, and used to refuse it from inside the comparison.
        if len(operands) == 1:
            return operands[0]
        return And(operands[0], *operands[1:])

    def __ne__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """
        Create a NOT(AND(...)) expression for inequality.

        For composite keys: NOT((a1 = b1) AND (a2 = b2) AND ...)
        """
        # Unequal is "not equal in every part", which is not "unequal in every
        # part": (1, 2) and (1, 3) differ. This used to be built of NotEqual
        # parts, NOT((a1 != b1) AND (a2 != b2)), by which a composite was
        # unequal to itself and equal to one it shares no part with.
        return Not(self == other)


# What a transform context returns for a field or a value of the domain: a
# node, or a composite of them.
Mapped = typing.Union[Visitable, CompositeExpression]
