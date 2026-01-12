"""Evaluate visitor for executing specification expressions."""
from typing import Any, Protocol, runtime_checkable

from .constants import OPERATOR_MAPPING
from .nodes import (
    Collection, Field, GlobalScope, Infix, Item, Object, Prefix, Value, Postfix,
)


@runtime_checkable
class Context(Protocol):
    """Context interface for retrieving values by key."""

    def get(self, key: str) -> Any:
        """Get value by key."""
        ...


class EvaluateVisitor:
    """Visitor that evaluates specification expressions."""

    _OPERATOR_MAPPING = OPERATOR_MAPPING

    def __init__(self, context: Context):
        self._context = context
        self._current_value: Any = None
        self._current_item: Context | None = None
        self._stack: list[Context] = []

    def _push(self, ctx: Context) -> None:
        """Push current context onto stack and set new context."""
        self._stack.append(self._context)
        self._context = ctx

    def _pop(self) -> None:
        """Restore previous context from stack."""
        self._context = self._stack.pop()

    def current_value(self) -> Any:
        """Return current evaluation result."""
        return self._current_value

    def set_current_value(self, val: Any) -> None:
        """Set current evaluation result."""
        self._current_value = val

    def visit_global_scope(self, node: GlobalScope) -> None:
        """Visit global scope node."""
        self._push(self._context)

    def visit_object(self, node: Object) -> None:
        """Visit object node and navigate to it."""
        node.parent().accept(self)
        obj = self._context.get(node.name())
        self._pop()
        if not isinstance(obj, Context):
            raise TypeError(f"Object {node.name()} is not a Context")
        self._push(obj)

    def visit_collection(self, node: Collection) -> None:
        """Visit collection node and evaluate predicate for each item."""
        node.parent().accept(self)
        items = self._context.get(node.name())
        self._pop()

        if not isinstance(items, list):
            raise TypeError("Value is not a collection of Contexts")

        result = False
        for item in items:
            if not isinstance(item, Context):
                raise TypeError("Collection item is not a Context")
            self._current_item = item
            node.predicate().accept(self)
            if not isinstance(self.current_value(), bool):
                raise TypeError("Predicate did not yield a boolean")
            result = result or self.current_value()

        self.set_current_value(result)

    def visit_item(self, node: Item) -> None:
        """Visit item node (current collection item)."""
        if self._current_item is None:
            raise RuntimeError("No current item in context")
        self._push(self._current_item)

    def visit_field(self, node: Field) -> None:
        """Visit field node and retrieve its value."""
        node.object().accept(self)
        value = self._context.get(node.name())
        self._pop()
        self.set_current_value(value)

    def visit_value(self, node: Value) -> None:
        """Visit value node."""
        self.set_current_value(node.value())

    def visit_prefix(self, node: Prefix) -> None:
        """Visit prefix operator node."""
        node.operand().accept(self)
        operand = self.current_value()
        self.set_current_value(self._OPERATOR_MAPPING[node.operator()](operand))

    def visit_infix(self, node: Infix) -> None:
        """Visit infix operator node."""
        node.left().accept(self)
        left = self.current_value()

        node.right().accept(self)
        right = self.current_value()
        self.set_current_value(self._OPERATOR_MAPPING[node.operator()](left, right))

    def visit_postfix(self, node: Postfix) -> None:
        """Visit postfix operator node."""
        node.operand().accept(self)
        operand = self.current_value()
        self.set_current_value(self._OPERATOR_MAPPING[node.operator()](operand))

    def result(self) -> bool:
        """Get final boolean result of evaluation."""
        result = self.current_value()
        if not isinstance(result, bool):
            raise TypeError("The result %r is not a bool", (result,))
        return result


class CollectionContext:
    """Context for collections that can be queried with wildcards."""

    def __init__(self, items: list[Context]):
        self._items = items

    def get(self, slice_: str) -> Any:
        """Get collection slice."""
        if slice_ == "*":
            return self._items
        raise ValueError(f'Unsupported slice type "{slice_}"')
