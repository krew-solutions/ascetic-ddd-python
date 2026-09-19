"""Evaluate visitor for executing specification expressions."""
from typing import Any, Protocol, runtime_checkable

from ascetic_ddd.specification.domain.constants import OPERATOR, OPERATOR_MAPPING
from ascetic_ddd.specification.domain.nodes import (
    Collection, Field, GlobalScope, Infix, Item, Object, Prefix,
    Value, Postfix, Visitor,
)


@runtime_checkable
class Context(Protocol):
    """Context interface for retrieving values by key."""

    def get(self, key: str) -> Any:
        """Get value by key."""
        ...


class EvaluateVisitor(Visitor[Any]):
    """Visitor that evaluates specification expressions.

    The logic of nulls is SQL's, so that this visitor and the database reading
    the query the same tree compiles to agree on every candidate:

    - a comparison or a computation with a null operand is null;
    - AND, OR, NOT are three-valued: ``NULL AND FALSE`` is false,
      ``NULL OR TRUE`` is true, ``NOT NULL`` is null;
    - IS, IS NULL, IS NOT NULL and a wildcard are true or false, never null;
    - a candidate satisfies a specification whose value is true - a null
      does not, as a row with a null condition is not selected.
    """

    _OPERATOR_MAPPING = OPERATOR_MAPPING

    # The value of the left operand that decides a connective whatever the
    # right one is. The right operand is then not evaluated, as it is not by
    # ``and``/``or`` of the lambda a specification may have been parsed from:
    # ``a != 0 and 10 / a > 1`` must not divide by zero in one of the two.
    _DECIDING = {
        OPERATOR.AND: False,
        OPERATOR.OR: True,
    }

    __slots__ = ('_context', '_current_item')

    def __init__(self, context: Context, current_item: Context | None = None):
        self._context = context
        self._current_item = current_item

    def _with_item(self, item: Context) -> 'EvaluateVisitor':
        """Return a sub-visitor bound to a new current item (for wildcard iteration)."""
        return EvaluateVisitor(self._context, item)

    def visit_global_scope(self, node: GlobalScope) -> Context:
        """Visit global scope node — return the root context."""
        return self._context

    def visit_object(self, node: Object) -> Context:
        """Visit object node — navigate to it from the parent context."""
        parent_ctx = node.parent().accept(self)
        obj = parent_ctx.get(node.name())
        if not isinstance(obj, Context):
            raise TypeError("Object %s is not a Context" % node.name())
        return obj

    def visit_collection(self, node: Collection) -> bool:
        """Visit collection node — whether some item satisfies the predicate.

        Stops at the first item that does, as ``any`` of a lambda does.
        """
        parent_ctx = node.parent().accept(self)
        items = parent_ctx.get(node.name())

        if not isinstance(items, list):
            raise TypeError("Value is not a collection of Contexts")

        return any(self._is_witness(node, item) for item in items)

    def _is_witness(self, node: Collection, item: Any) -> bool:
        """Whether ``item`` satisfies the predicate of ``node``."""
        if not isinstance(item, Context):
            raise TypeError("Collection item is not a Context")
        value = node.predicate().accept(self._with_item(item))
        # As in EXISTS (... WHERE predicate): an item whose predicate is null
        # is not a witness, and the wildcard itself is never null.
        return self._truth(value, "Predicate did not yield a boolean") is True

    def _truth(self, value: Any, message: str) -> bool | None:
        """Return ``value``, which must be true, false, or null - unknown."""
        if value is None or isinstance(value, bool):
            return value
        raise TypeError("%s, got: %s" % (message, type(value).__name__))

    def visit_item(self, node: Item) -> Context:
        """Visit item node (current collection item)."""
        if self._current_item is None:
            raise RuntimeError("No current item in context")
        return self._current_item

    def visit_field(self, node: Field) -> Any:
        """Visit field node — retrieve its value from the object context."""
        obj_ctx = node.object().accept(self)
        return obj_ctx.get(node.name())

    def visit_value(self, node: Value) -> Any:
        """Visit value node — return the literal."""
        return node.value()

    def visit_prefix(self, node: Prefix) -> Any:
        """Visit prefix operator node."""
        operand = node.operand().accept(self)
        if node.operator() is OPERATOR.NOT:
            operand = self._truth(operand, 'Operator "NOT" requires a boolean')
        if operand is None:
            return None
        return self._OPERATOR_MAPPING[node.operator()](operand)

    def visit_infix(self, node: Infix) -> Any:
        """Visit infix operator node."""
        if node.operator() in self._DECIDING:
            return self._visit_connective(node)
        left = node.left().accept(self)
        right = node.right().accept(self)
        if node.operator() is OPERATOR.IS:
            # Equality in which null is a value: two nulls are equal, a null
            # and a value are not. Never null.
            if left is None or right is None:
                return left is None and right is None
        elif left is None or right is None:
            return None
        return self._OPERATOR_MAPPING[node.operator()](left, right)

    def _visit_connective(self, node: Infix) -> bool | None:
        """Visit AND or OR: three-valued, and stopping when decided."""
        message = 'Operator "%s" requires a boolean' % node.operator().value
        deciding = self._DECIDING[node.operator()]
        left = self._truth(node.left().accept(self), message)
        if left is deciding:
            return deciding
        right = self._truth(node.right().accept(self), message)
        if right is deciding:
            return deciding
        if left is None or right is None:
            return None
        return not deciding

    def visit_postfix(self, node: Postfix) -> Any:
        """Visit postfix operator node."""
        operand = node.operand().accept(self)
        return self._OPERATOR_MAPPING[node.operator()](operand)


class CollectionContext:
    """Context for collections that can be queried with wildcards."""

    def __init__(self, items: list[Context]):
        self._items = items

    def get(self, slice_: str) -> Any:
        """Get collection slice."""
        if slice_ == "*":
            return self._items
        raise ValueError(f'Unsupported slice type "{slice_}"')
