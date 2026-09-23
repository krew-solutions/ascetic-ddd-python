"""Transform visitor for converting domain specifications to infrastructure specifications."""
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, List, NamedTuple, Optional

from ascetic_ddd.option import Option

from ascetic_ddd.specification.domain.nodes import (
    Collection, EmptiableObject, Field, GlobalScope, Infix, IsNotNull, IsNull, Item, Object,
    Prefix,
    Value, Visitable, Postfix, Visitor, extract_field_path, extract_field_root,
    extract_object_path, extract_object_root, read_option,
)

__all__ = (
    'CompositeExpressionsDifferentLengthError',
    'IMapping',
    'MappingVisitor',
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


class IMapping(metaclass=ABCMeta):
    """
    The mapping: what a domain's members and values are in the storage,
    Fowler's Metadata Mapping for a Query Object.

    A mapping is of
    the aggregate's members and knows nothing of any query: it is asked
    about a member by its whole path from the candidate,
    ``["categories", "products", "price"]`` for the price of a product of a
    category, and answers what that is in the storage as a path from the
    candidate's row, ``categories.products.price_cents``. A collection is a
    member like any other, ``["categories", "products"]``. Where a query
    stands when it asks - inside which collection's predicate, how far out an
    item is - is the tree's, and the transformer puts the answer there: the
    member of an item is the answer less the collection's, from the item.

    It used to ask about the members of "the item" by their names alone,
    ``item_attr_node``, so a mapping could not tell the items of one
    collection from another's; and about where a collection is kept, which
    is the schema's to say.

    A mapping is a guard as well. A specification may arrive as data - a
    template bound from a request, a tree from another service - and the
    mapping names every member such a specification may filter by, and
    refuses any other: a member the mapping does not know is its error, not
    a column that happens to exist. So what reaches the database is a query
    over the columns the repository chose to expose, through the relations
    it declared in the schema, and not whatever a caller composed to read
    another table or to scan a column without an index. The transformer is
    the one place for that: the compiler writes the names it is given. The
    size of a tree is bounded by the parsers - its height and its nesting -
    and its members by the mapping.

    To inherit: both methods are abstract, so a mapping that does not say
    them cannot be made. It used to be a Protocol, which cannot have that:
    what a mapping lacked was found by the first specification that needed
    it.
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



class _Collection(NamedTuple):
    """A collection whose predicate is being transformed: its whole path from
    the candidate, in the domain's names and in the storage's."""
    domain: tuple[str, ...]
    storage: tuple[str, ...]


def _placed(mapped: Mapped, collection: _Collection, depth: int) -> Mapped:
    """Return the storage's path of a member, put where the member was.

    Of a member of an item the mapping's answer is a path from the
    candidate, which starts with the collection's; the rest is the member
    from the item, ``depth`` collections out. A part of a composite is put
    so as a whole, and a value is what it is.

    Raises:
        ValueError: If the answer does not start with the collection's
    """
    if isinstance(mapped, CompositeExpression):
        return CompositeExpression(*[_placed(part, collection, depth) for part in mapped.nodes()])
    if isinstance(mapped, Field) and isinstance(extract_field_root(mapped), GlobalScope):
        names = extract_field_path(mapped)
        prefix = list(collection.storage)
        if names[:len(prefix)] != prefix or len(names) == len(prefix):
            raise ValueError(
                "The mapping put a member of an item outside its collection: %s is not under %s"
                % (".".join(names), ".".join(prefix))
            )
        return Field(_object_chain(Item(depth), names[len(prefix):-1]), names[-1])
    if isinstance(mapped, (Field, Value)):
        return mapped
    if isinstance(mapped, Prefix):
        return Prefix(mapped.operator(), _node(_placed(mapped.operand(), collection, depth)), mapped.associativity())
    if isinstance(mapped, Postfix):
        return Postfix(_node(_placed(mapped.operand(), collection, depth)), mapped.operator(), mapped.associativity())
    if isinstance(mapped, Infix):
        return Infix(
            _node(_placed(mapped.left(), collection, depth)),
            mapped.operator(),
            _node(_placed(mapped.right(), collection, depth)),
            mapped.associativity(),
        )
    return mapped


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


def transform(context: IMapping, expression: Visitable) -> Visitable:
    """
    Transform a domain specification to an infrastructure specification.

    Args:
        context: Transform context for mapping domain to infrastructure
        expression: Domain specification expression

    Returns:
        Infrastructure specification expression: a node. ``accept`` of a
        MappingVisitor returns what a part of the tree is mapped to, which
        may be a composite; of the whole tree it may not.

    Raises:
        ValueError: If the specification as a whole is a composite
    """
    return _node(expression.accept(MappingVisitor(context)))


class MappingVisitor(Visitor[Mapped]):
    """
    Visitor that applies a mapping: the domain's specification tree becomes
    the storage's, its members and values as the mapping has them.

    Handles:
    - Field path mapping (e.g., "id" -> ["tenant_id", "member_id"])
    - Value object decomposition (e.g., CompositeId -> individual values)
    - Composite expression support for composite keys

    Each ``visit_*`` returns what its node is mapped to: a node, or - for a
    field or a value that the context maps so - a composite, which the
    ``visit_infix`` above it turns into nodes.
    """

    def __init__(self, context: IMapping, _inside: tuple[_Collection, ...] = ()):
        self._context = context
        # The collections the expression is inside of, the nearest last: the
        # item ``depth`` collections out is of ``_inside[-1 - depth]``.
        self._inside = _inside

    def _collection(self, item: Item) -> _Collection:
        """Return the collection whose item ``item`` is.

        Raises:
            ValueError: If there is no collection that far out
        """
        if item.depth() >= len(self._inside):
            raise ValueError("No current item in context: the item %d collections out" % item.depth())
        return self._inside[-1 - item.depth()]

    def _whole(self, root: EmptiableObject, names: list[str]) -> list[str]:
        """Return the whole path from the candidate of the member at ``names`` from ``root``."""
        if isinstance(root, Item):
            return list(self._collection(root).domain) + names
        return names

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
        # A collection is a member: the context says where it is, by a path
        # from the candidate, and the predicate is transformed inside it.
        root = extract_object_root(node.parent())
        domain = self._whole(root, extract_object_path(node.parent()))
        mapped = self._context.attr_node(domain)
        if not isinstance(mapped, Field) or not isinstance(extract_field_root(mapped), GlobalScope):
            raise ValueError(
                "The mapping answered for a collection with what is not a place: %s" % ".".join(domain)
            )
        storage = extract_field_path(mapped)
        collection = _Collection(tuple(domain), tuple(storage))
        if isinstance(root, Item):
            placed = _placed(mapped, self._collection(root), root.depth())
            assert isinstance(placed, Field)
            parent: EmptiableObject = Object(placed.object(), placed.name())
        else:
            parent = _object_chain(GlobalScope(), storage)
        inside = MappingVisitor(self._context, self._inside + (collection,))
        return Collection(parent, node.name(), _node(node.predicate().accept(inside)))

    def visit_item(self, node: Item) -> Mapped:
        """Visit item node — passthrough."""
        return node

    def visit_field(self, node: Field) -> Mapped:
        """
        Visit field node and transform to infrastructure field(s).

        Extracts the field path and uses context to map it to infrastructure.
        May return a composite expression for composite keys.
        """
        root = extract_field_root(node)
        mapped = self._context.attr_node(self._whole(root, extract_field_path(node)))
        if isinstance(root, Item):
            return _placed(mapped, self._collection(root), root.depth())
        return mapped

    def visit_value(self, node: Value) -> Mapped:
        """
        Visit value node and transform to infrastructure value(s).

        Uses context to decompose value objects into database-compatible values.
        May return a composite expression for composite value objects.
        """
        # An Option of a value is the value, which the context maps, or the
        # null it is in any storage: a context is asked of the domain's values.
        value = read_option(node.value())
        if value is None and isinstance(node.value(), Option):
            return Value(None)
        return self._context.value_node(value)

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
                isinstance(operand, Value) and read_option(operand.value()) is not None
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
