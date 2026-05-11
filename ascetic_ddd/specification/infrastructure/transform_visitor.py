"""Transform visitor for converting domain specifications to infrastructure specifications."""
from typing import Any, List, Protocol

from ascetic_ddd.specification.domain.nodes import (
    Collection, Field, GlobalScope, Infix, Item, Object, Prefix, Value, Visitable,
    Postfix, Visitor, extract_field_path,
)
from ascetic_ddd.specification.domain.constants import OPERATOR, OPERATOR_MAPPING

from ascetic_ddd.specification.infrastructure.composite_expression_node import CompositeExpression


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


class TransformVisitor(Visitor[Visitable]):
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

    def visit_global_scope(self, node: GlobalScope) -> Visitable:
        """Visit global scope node — passthrough."""
        return node

    def visit_object(self, node: Object) -> Visitable:
        """Visit object node — passthrough."""
        return node

    def visit_collection(self, node: Collection) -> Visitable:
        """Visit collection node — passthrough."""
        return node

    def visit_item(self, node: Item) -> Visitable:
        """Visit item node — passthrough."""
        return node

    def visit_field(self, node: Field) -> Visitable:
        """
        Visit field node and transform to infrastructure field(s).

        Extracts the field path and uses context to map it to infrastructure.
        May return a composite expression for composite keys.
        """
        return self._context.attr_node(extract_field_path(node))

    def visit_value(self, node: Value) -> Visitable:
        """
        Visit value node and transform to infrastructure value(s).

        Uses context to decompose value objects into database-compatible values.
        May return a composite expression for composite value objects.
        """
        return self._context.value_node(node.value())

    def visit_prefix(self, node: Prefix) -> Visitable:
        """
        Visit prefix node (e.g., NOT).

        Recursively transforms the operand and wraps in prefix operator.
        """
        operand = node.operand().accept(self)
        return Prefix(node.operator(), operand, node.associativity())

    def visit_infix(self, node: Infix) -> Visitable:
        """
        Visit infix node (e.g., AND, OR, =, >).

        Recursively transforms left and right operands.
        Special handling for composite expressions with equality/inequality.
        """
        left = node.left().accept(self)
        right = node.right().accept(self)

        # Check if we have composite expressions
        if isinstance(left, CompositeExpression):
            if not isinstance(right, CompositeExpression):
                raise CompositeExpressionsDifferentLengthError(
                    "Not enough composite expressions"
                )

            # Handle composite expression operators
            if node.operator() not in (OPERATOR.EQ, OPERATOR.NE):
                raise ValueError(
                    'Operator "%s" is not supported for composite expressions'
                    % node.operator()
                )
            return self._OPERATOR_MAPPING[node.operator()](left, right)

        # Regular infix operation
        return Infix(left, node.operator(), right, node.associativity())

    def visit_postfix(self, node: Postfix) -> Visitable:
        """
        Visit postfix node (e.g., IS NULL).

        Recursively transforms the operand and wraps in postfix operator.
        """
        operand = node.operand().accept(self)
        return Postfix(operand, node.operator(), node.associativity())
