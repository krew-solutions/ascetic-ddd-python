"""A tree as plain data, so that a test can compare two trees for equality.

The nodes define no ``__eq__``; the tests of the parsers compare trees field
by field. ``describe`` folds a tree into nested tuples instead, which compare
structurally and print readably when an assertion fails.
"""
from typing import Any

from ascetic_ddd.specification.domain.nodes import (
    Collection, Field, GlobalScope, Infix, Item, Object, Postfix,
    Prefix, Value, Visitable, Visitor,
)

__all__ = ('describe',)


class _DescribeVisitor(Visitor[Any]):
    """Stateless: every ``visit_*`` returns the description of its node."""

    def visit_global_scope(self, node: GlobalScope) -> Any:
        return "$"

    def visit_item(self, node: Item) -> Any:
        return "@" if node.depth() == 0 else "@%d" % node.depth()

    def visit_object(self, node: Object) -> Any:
        return (node.parent().accept(self), node.name())

    def visit_field(self, node: Field) -> Any:
        return ("field", node.object().accept(self), node.name())

    def visit_value(self, node: Value) -> Any:
        return ("value", node.value())

    def visit_collection(self, node: Collection) -> Any:
        return ("any", node.parent().accept(self), node.predicate().accept(self))

    def visit_prefix(self, node: Prefix) -> Any:
        return (node.operator().name, node.operand().accept(self))

    def visit_infix(self, node: Infix) -> Any:
        return (node.operator().name, node.left().accept(self), node.right().accept(self))

    def visit_postfix(self, node: Postfix) -> Any:
        return (node.operator().name, node.operand().accept(self))


def describe(node: Visitable) -> Any:
    """Describe ``node`` as nested tuples.

    Args:
        node: The root of a specification tree.

    Returns:
        ``("GT", ("field", "$", "age"), ("value", 18))`` for ``age > 18``.
    """
    return node.accept(_DescribeVisitor())
