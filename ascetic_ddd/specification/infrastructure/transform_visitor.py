"""Transform visitor for converting domain specifications to infrastructure specifications."""
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, List, Optional

from ascetic_ddd.specification.domain.nodes import (
    Collection, EmptiableObject, Field, GlobalScope, Infix, IsNotNull, IsNull, Item, Object,
    Prefix,
    Value, Visitable, Postfix, Visitor, extract_field_path, extract_field_root,
    extract_object_path, extract_object_root,
)

__all__ = (
    'CompositeExpressionsDifferentLengthError',
    'ITransformContext',
    'TransformVisitor',
    'transform',
)
from ascetic_ddd.specification.domain.constants import OPERATOR

from ascetic_ddd.specification.infrastructure.composite_expression_node import (
    CompositeExpression,
    CompositeExpressionsDifferentLengthError,
    Mapped,
)


def _object_chain(root: EmptiableObject, names: List[str]) -> EmptiableObject:
    """Build the object the names lead to from the root: the same place."""
    result = root
    for name in names:
        result = Object(result, name)
    return result


class ITransformContext(metaclass=ABCMeta):
    """
    Interface for transformation context.

    What a domain's fields and values are in the storage. To inherit: what
    every mapping must say is abstract, so a mapping that does not say it
    cannot be made; what a mapping may say has an answer here, which a
    mapping that has nothing to add leaves alone.

    It used to be a Protocol, which can have neither: what a mapping lacked
    was found by the first specification that needed it, and what it may
    leave out had to be asked about through interfaces of its own.

    Mark what a mapping overrides with ``typing.override``: a misspelled
    ``collection_node`` is then an error of the type checker, and not the
    answer of the interface silently kept.
    """

    @abstractmethod
    def attr_node(self, path: List[str]) -> Mapped:
        """Transform domain field path to infrastructure node, or to a
        composite of them."""
        raise NotImplementedError

    @abstractmethod
    def value_node(self, val: Any) -> Mapped:
        """Transform domain value to infrastructure node, or to a composite
        of them."""
        raise NotImplementedError

    def item_attr_node(self, path: List[str]) -> Mapped:
        """Transform domain field path of the current item of a collection
        to infrastructure node.

        A path from Item() and a path from GlobalScope() can be of the same
        names, and are not the same field: a mapping is asked about each by
        a method of its own. Needed only by a context of specifications that
        have collections, so it is not abstract; and it has no answer but
        the mapping's: left as it is, the field would reach the query under
        the domain's name.

        Raises:
            NotImplementedError: Unless the mapping says what the field is
        """
        raise NotImplementedError(
            "%s does not map the fields of an item of a collection: %s"
            % (type(self).__name__, ".".join(path))
        )

    def collection_node(self, path: List[str]) -> EmptiableObject:
        """Transform domain path of a collection of the candidate to the
        infrastructure object its items are of.

        The same place, unless the mapping says otherwise.

        Example:
            ["parts"] -> Object(GlobalScope(), "something_parts")
        """
        return _object_chain(GlobalScope(), path)

    def item_collection_node(self, path: List[str]) -> EmptiableObject:
        """Transform domain path of a collection of the current item to the
        infrastructure object its items are of.

        The same place, unless the mapping says otherwise.

        Example:
            ["parts"] -> Object(Item(), "sub_parts")
        """
        return _object_chain(Item(), path)


def _node(mapped: Mapped) -> Visitable:
    """
    Return a node, which a composite is not.

    A composite stands for several expressions, and is given a meaning by
    `=` and `!=` alone. Where one node is needed - under any other operator,
    as a predicate, as the whole specification - it used to go into the tree,
    and raised NotImplementedError from inside whatever visited the tree next.

    Raises:
        ValueError: If ``mapped`` is a composite
    """
    if isinstance(mapped, CompositeExpression):
        raise ValueError(
            "A composite expression where a single one is needed:"
            " only = and != compare composites"
        )
    return mapped


def transform(context: ITransformContext, expression: Visitable) -> Visitable:
    """
    Transform a domain specification to an infrastructure specification.

    Args:
        context: Transform context for mapping domain to infrastructure
        expression: Domain specification expression

    Returns:
        Infrastructure specification expression: a node. ``accept`` of a
        TransformVisitor returns what a part of the tree is mapped to, which
        may be a composite; of the whole tree it may not.

    Raises:
        ValueError: If the specification as a whole is a composite
    """
    return _node(expression.accept(TransformVisitor(context)))


class TransformVisitor(Visitor[Mapped]):
    """
    Visitor that transforms domain specification AST to infrastructure specification AST.

    Handles:
    - Field path mapping (e.g., "id" -> ["tenant_id", "member_id"])
    - Value object decomposition (e.g., CompositeId -> individual values)
    - Composite expression support for composite keys

    Each ``visit_*`` returns what its node is mapped to: a node, or - for a
    field or a value that the context maps so - a composite, which the
    ``visit_infix`` above it turns into nodes.
    """

    def __init__(self, context: ITransformContext):
        self._context = context

    def visit_global_scope(self, node: GlobalScope) -> Mapped:
        """Visit global scope node — passthrough."""
        return node

    def visit_object(self, node: Object) -> Mapped:
        """Visit object node — passthrough."""
        return node

    def visit_collection(self, node: Collection) -> Mapped:
        """
        Visit collection node.

        Recursively transforms the predicate. It used to be a passthrough, so
        the values of a predicate reached the query as the domain's objects,
        and its fields under the domain's names.
        """
        return Collection(
            self._transform_collection_parent(node.parent()),
            node.name(),
            _node(node.predicate().accept(self)),
        )

    def _transform_collection_parent(self, parent: EmptiableObject) -> EmptiableObject:
        """
        Transform the object a collection is of to where the context says it is kept.

        It used to stay under the domain's name whatever the storage calls
        it: `unnest(parts)` of a column that is `something_parts`.
        """
        if isinstance(extract_object_root(parent), Item):
            return self._context.item_collection_node(extract_object_path(parent))
        return self._context.collection_node(extract_object_path(parent))

    def visit_item(self, node: Item) -> Mapped:
        """Visit item node — passthrough."""
        return node

    def visit_field(self, node: Field) -> Mapped:
        """
        Visit field node and transform to infrastructure field(s).

        Extracts the field path and uses context to map it to infrastructure.
        May return a composite expression for composite keys.
        """
        if isinstance(extract_field_root(node), Item):
            return self._context.item_attr_node(extract_field_path(node))
        return self._context.attr_node(extract_field_path(node))

    def visit_value(self, node: Value) -> Mapped:
        """
        Visit value node and transform to infrastructure value(s).

        Uses context to decompose value objects into database-compatible values.
        May return a composite expression for composite value objects.
        """
        return self._context.value_node(node.value())

    def visit_prefix(self, node: Prefix) -> Mapped:
        """
        Visit prefix node (e.g., NOT).

        Recursively transforms the operand and wraps in prefix operator.
        """
        operand = _node(node.operand().accept(self))
        return Prefix(node.operator(), operand, node.associativity())

    def _null_test(self, node: Infix, left: Mapped, right: Mapped) -> Optional[Visitable]:
        """
        Return the null test an equality stands for, if it stands for one.

        A value of the domain that the storage keeps as a null - a special
        case that answers for itself, ``discount == NoDiscount()`` - is equal
        to itself in the domain, and ``discount = $1`` with a null is true of
        nothing. It is tested for: IS NULL, and IS NOT NULL of ``!=``.

        Only a null the mapping made: a value that was null in the domain
        already stays compared, as it is in the tree. The context maps
        operands and knows nothing of operators; it is here, where both
        operands are mapped and the node is built, that the operator is seen.

        Args:
            node: The comparison, in the domain's terms
            left: Its left operand, mapped
            right: Its right operand, mapped
        """
        if node.operator() is OPERATOR.EQ:
            test: Callable[[Visitable], Visitable] = IsNull
        elif node.operator() is OPERATOR.NE:
            test = IsNotNull
        else:
            return None
        for operand, mapped, other in ((node.right(), right, left), (node.left(), left, right)):
            made_null = (
                isinstance(operand, Value) and operand.value() is not None
                and isinstance(mapped, Value) and mapped.value() is None
            )
            if made_null and not isinstance(other, CompositeExpression):
                return test(other)
        return None

    def visit_infix(self, node: Infix) -> Mapped:
        """
        Visit infix node (e.g., AND, OR, =, >).

        Recursively transforms left and right operands.
        Special handling for composite expressions with equality/inequality.
        """
        left = node.left().accept(self)
        right = node.right().accept(self)

        # Equality with a value the mapping made the storage's null is the
        # null test of the other operand
        tested = self._null_test(node, left, right)
        if tested is not None:
            return tested

        # Check if we have composite expressions, on either side: one on the
        # right alone used to go into the tree as it was
        if isinstance(left, CompositeExpression) or isinstance(right, CompositeExpression):
            if not isinstance(left, CompositeExpression) or not isinstance(right, CompositeExpression):
                raise CompositeExpressionsDifferentLengthError(
                    "Not enough composite expressions"
                )

            # Handle composite expression operators. They are the composite's
            # own, and not those of the evaluator's table of operators, which
            # compute and compare values.
            if node.operator() is OPERATOR.EQ:
                return left == right
            if node.operator() is OPERATOR.NE:
                return left != right
            raise ValueError(
                'Operator "%s" is not supported for composite expressions'
                % node.operator()
            )

        # Regular infix operation
        return Infix(left, node.operator(), right, node.associativity())

    def visit_postfix(self, node: Postfix) -> Mapped:
        """
        Visit postfix node (e.g., IS NULL).

        Recursively transforms the operand and wraps in postfix operator.
        """
        operand = _node(node.operand().accept(self))
        return Postfix(operand, node.operator(), node.associativity())
