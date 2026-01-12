"""Transform visitor for converting domain specifications to infrastructure specifications."""
from typing import Any, List, Protocol

from ..domain.nodes import (
    Visitor, Collection, Field, GlobalScope, Infix, Item, Object, Prefix, Value, Visitable, And,
    extract_field_path, Infix, Not, Postfix,
)
from ..domain.constants import OPERATOR, OPERATOR_MAPPING

from .composite_expression_node import CompositeExpression


class CompositeExpressionsDifferentLengthError(Exception):
    """Raised when composite expressions have different lengths."""

    pass


class ITransformContext(Protocol):
    """Interface for transformation context."""

    def attr_node(self, path: List[str]) -> Visitable:
        """Transform domain field path to infrastructure node."""
        ...

    def value_node(self, val: Any) -> Visitable:
        """Transform domain value to infrastructure node."""
        ...


class TransformVisitor(Visitor):
    """
    Visitor that transforms domain specification AST to infrastructure specification AST.

    Handles:
    - Field path mapping (e.g., "id" -> ["tenant_id", "member_id"])
    - Value object decomposition (e.g., CompositeId -> individual values)
    - Composite expression support for composite keys
    """

    _OPERATOR_MAPPING = OPERATOR_MAPPING

    def __init__(self, context: ITransformContext):
        self._context = context
        self._current_node: Visitable | None = None
        self._stack: List[ITransformContext] = []

    def push(self, ctx: ITransformContext) -> None:
        """Push current context onto stack and switch to new context."""
        self._stack.append(self._context)
        self._context = ctx

    def pop(self) -> None:
        """Restore previous context from stack."""
        self._context = self._stack[-1]
        self._stack = self._stack[:-1]

    def visit_global_scope(self, node: GlobalScope) -> None:
        """Visit global scope node."""
        # Context push/pop handled at higher level if needed
        pass

    def visit_object(self, node: Object) -> None:
        """Visit object node."""
        pass

    def visit_collection(self, node: Collection) -> None:
        """Visit collection node."""
        pass

    def visit_item(self, node: Item) -> None:
        """Visit item node."""
        # Context push/pop handled at higher level if needed
        pass

    def visit_field(self, node: Field) -> None:
        """
        Visit field node and transform to infrastructure field(s).

        Extracts the field path and uses context to map it to infrastructure.
        May return a composite expression for composite keys.
        """
        path = extract_field_path(node)
        self._current_node = self._context.attr_node(path)

    def visit_value(self, node: Value) -> None:
        """
        Visit value node and transform to infrastructure value(s).

        Uses context to decompose value objects into database-compatible values.
        May return a composite expression for composite value objects.
        """
        self._current_node = self._context.value_node(node.value())

    def visit_prefix(self, node: Prefix) -> None:
        """
        Visit prefix node (e.g., NOT).

        Recursively transforms the operand and wraps in prefix operator.
        """
        node.operand().accept(self)
        self._current_node = Prefix(
            node.operator(), self._current_node, node.associativity()
        )

    def visit_infix(self, node: Infix) -> None:
        """
        Visit infix node (e.g., AND, OR, =, >).

        Recursively transforms left and right operands.
        Special handling for composite expressions with equality/inequality.
        """
        # Transform left operand
        node.left().accept(self)
        left = self._current_node

        # Transform right operand
        node.right().accept(self)
        right = self._current_node

        # Check if we have composite expressions
        if isinstance(left, CompositeExpression):
            if not isinstance(right, CompositeExpression):
                raise CompositeExpressionsDifferentLengthError(
                    "Not enough composite expressions"
                )

            # Handle composite expression operators
            if node.operator() not in (OPERATOR.EQ, OPERATOR.NE):
                raise ValueError(
                    f'Operator "{node.operator()}" is not supported for composite expressions'
                )
            self._current_node = self._OPERATOR_MAPPING[node.operator()](left, right)
        else:
            # Regular infix operation
            self._current_node = Infix(
                left, node.operator(), right, node.associativity()
            )

    def visit_postfix(self, node: Postfix) -> None:
        """
        Visit postfix node (e.g., IS NULL).

        Recursively transforms the operand and wraps in postfix operator.
        """
        node.operand().accept(self)
        self._current_node = Postfix(
            node.operator(), self._current_node, node.associativity()
        )

    def result(self) -> Visitable:
        """Return the transformed AST."""
        return self._current_node
