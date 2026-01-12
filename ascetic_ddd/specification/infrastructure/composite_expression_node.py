"""Composite expression node for handling composite keys."""
from typing import Protocol

from ..domain.nodes import (
    Visitable,
    Visitor,
    And,
    Equal,
    Not,
    NotEqual,
)


class CompositeExpressionsDifferentLengthError(Exception):
    """Raised when composite expressions have different lengths."""

    pass


class ICompositeExpression(Protocol):
    """Interface for expression composers."""

    def __eq__(self, other: "CompositeExpression") -> Visitable:
        """Create equality expression with another composite."""
        ...

    def __ne__(self, other: "CompositeExpression") -> Visitable:
        """Create not-equal expression with another composite."""
        ...

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        ...


class CompositeExpression(Visitable):
    """Node representing a composite expression (e.g., composite key)."""

    def __init__(self, *nodes: Visitable):
        self._nodes = list(nodes)

    def __eq__(self, other: "CompositeExpression") -> Visitable:
        """
        Create an AND expression of equality comparisons.

        For composite keys: (a1 = b1) AND (a2 = b2) AND ...
        """
        if len(self._nodes) != len(other._nodes):
            raise CompositeExpressionsDifferentLengthError(
                "Composite expressions have different length"
            )

        operands = []
        for i in range(len(self._nodes)):
            left, right = self._nodes[i], other._nodes[i]

            if isinstance(left, CompositeExpression):
                if not isinstance(right, CompositeExpression):
                    raise CompositeExpressionsDifferentLengthError(
                        "Composite expressions have different length"
                    )
                new_node = left == right
                operands.append(new_node)
            else:
                operands.append(Equal(left, right))

        return And(operands[0], *operands[1:])

    def __ne__(self, other: "CompositeExpression") -> Visitable:
        """
        Create a NOT(AND(...)) expression for inequality.

        For composite keys: NOT((a1 = b1) AND (a2 = b2) AND ...)
        """
        if len(self._nodes) != len(other._nodes):
            raise CompositeExpressionsDifferentLengthError(
                "Composite expressions have different length"
            )

        operands = []
        for i in range(len(self._nodes)):
            left, right = self._nodes[i], other._nodes[i]

            if isinstance(left, CompositeExpression):
                if not isinstance(right, CompositeExpression):
                    raise CompositeExpressionsDifferentLengthError(
                        "Composite expressions have different length"
                    )
                new_node = left != right
                operands.append(new_node)
            else:
                operands.append(NotEqual(left, right))

        return Not(And(operands[0], *operands[1:]))

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor (no-op for composite nodes)."""
        pass
