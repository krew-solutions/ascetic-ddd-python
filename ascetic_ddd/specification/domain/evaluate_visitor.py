"""Evaluate visitor for executing specification expressions."""
from typing import Any, Protocol, runtime_checkable

from ascetic_ddd.specification.domain.constants import OPERATOR_MAPPING
from ascetic_ddd.specification.domain.nodes import (
    Collection, Field, GlobalScope, Infix, Item, Object, Prefix, Value, Postfix,
    Visitor,
)


@runtime_checkable
class Context(Protocol):
    """Context interface for retrieving values by key."""

    def get(self, key: str) -> Any:
        """Get value by key."""
        ...


class EvaluateVisitor(Visitor[Any]):
    """Visitor that evaluates specification expressions."""

    _OPERATOR_MAPPING = OPERATOR_MAPPING

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
        """Visit collection node — evaluate predicate for each item; OR-aggregate."""
        parent_ctx = node.parent().accept(self)
        items = parent_ctx.get(node.name())

        if not isinstance(items, list):
            raise TypeError("Value is not a collection of Contexts")

        result = False
        for item in items:
            if not isinstance(item, Context):
                raise TypeError("Collection item is not a Context")
            value = node.predicate().accept(self._with_item(item))
            if not isinstance(value, bool):
                raise TypeError("Predicate did not yield a boolean")
            result = result or value
        return result

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
        return self._OPERATOR_MAPPING[node.operator()](operand)

    def visit_infix(self, node: Infix) -> Any:
        """Visit infix operator node."""
        left = node.left().accept(self)
        right = node.right().accept(self)
        return self._OPERATOR_MAPPING[node.operator()](left, right)

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
