"""Composite expression node for handling composite keys."""
import typing
from typing import Protocol

from ascetic_ddd.specification.domain.nodes import (
    Visitable,
    Visitor,
    And,
    Equal,
    Not,
    NotEqual,
)


T = typing.TypeVar("T")


class CompositeExpressionsDifferentLengthError(Exception):
    """Raised when composite expressions have different lengths."""

    pass


class ICompositeExpression(Protocol):
    """Interface for expression composers."""

    def __eq__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """Create equality expression with another composite."""
        ...

    def __ne__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
        """Create not-equal expression with another composite."""
        ...

    def accept(self, visitor: Visitor[T]) -> T:
        """Accept a visitor."""
        ...


class CompositeExpression(Visitable):
    """Node representing a composite expression (e.g., composite key)."""

    def __init__(self, *nodes: Visitable):
        self._nodes = list(nodes)

    def __eq__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
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

    def __ne__(self, other: "CompositeExpression") -> Visitable:  # type: ignore[override]
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

    def accept(self, visitor: Visitor[T]) -> T:
        """Accept a visitor.

        CompositeExpression is a structural marker for composite keys: it never
        participates in dispatch — ``TransformVisitor`` inspects it via
        ``isinstance`` and handles it directly. This method exists solely to
        satisfy the ``Visitable`` Protocol and must not be invoked.
        """
        raise NotImplementedError(
            "CompositeExpression.accept is not part of visitor dispatch — "
            "it is handled structurally by TransformVisitor.visit_infix"
        )
