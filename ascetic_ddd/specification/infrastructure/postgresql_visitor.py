"""PostgreSQL visitor for generating SQL from specification AST."""
import dataclasses
import re

import inflection
from typing import Any, List, Optional, Tuple

from ascetic_ddd.specification.domain.nodes import (
    Visitor,
    Collection,
    Field,
    GlobalScope,
    Infix,
    Item,
    Object,
    Operable,
    Prefix,
    Postfix,
    Value,
    Visitable,
    EmptiableObject,
    extract_field_path,
    extract_field_root,
)
from ascetic_ddd.specification.domain.constants import ASSOCIATIVITY, OPERATOR

from ascetic_ddd.specification.infrastructure.transform_visitor import ITransformContext, transform
from ascetic_ddd.specification.infrastructure.schema import SchemaRegistry


SqlFragment = Tuple[str, List[Any]]


def compile_specification(
    context: ITransformContext,
    expression: Visitable,
    schema: Optional[SchemaRegistry] = None,
) -> SqlFragment:
    """
    Compile a domain specification to SQL: what a repository does with one.

    The context says what the members and the values of the domain are in
    the storage, the schema how the storage is laid out, and both are the
    repository's to know: a query cannot be written without knowing the
    table. The schema used not to be taken, so a mapping and a schema could
    not be given together.

    Args:
        context: Transform context for mapping domain to infrastructure
        expression: Domain specification expression
        schema: Optional schema registry for relational collection support

    Returns:
        Tuple of (sql_string, parameters)
    """
    # First, transform domain expression to infrastructure expression
    infrastructure_expr = transform(context, expression)

    # Then, generate SQL from infrastructure expression
    return infrastructure_expr.accept(PostgresqlVisitor(schema=schema))


def compile_to_sql(
    expression: Visitable,
    schema: Optional[SchemaRegistry] = None
) -> SqlFragment:
    """
    Compile AST directly to SQL without context transformation.

    For a tree that is in the storage's names already. A tree parsed of a
    predicate over the domain's objects is not: it goes through
    ``compile_specification``.

    Args:
        expression: Specification expression AST
        schema: Optional schema registry for relational collection support

    Returns:
        Tuple of (sql_string, parameters)
    """
    return expression.accept(PostgresqlVisitor(schema=schema))


@dataclasses.dataclass
class _Counters:
    """Shared monotonic counters across the visitor tree.

    These cross sub-visitor boundaries: each new $-placeholder and each new
    wildcard alias must get a globally-unique number, so they live in a
    mutable container that is shared by reference among all sub-visitors.
    """
    placeholder_index: int = 0
    wildcard_counter: int = 0


def _build_precedence_mapping() -> dict[str, int]:
    """
    Setup PostgreSQL operator precedence.

    Based on: https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-PRECEDENCE-TABLE
    Higher numbers = higher precedence.
    """
    mapping: dict[str, int] = {}

    def assign(precedence: int, *operators: str) -> None:
        for op in operators:
            mapping[op] = precedence

    assign(160, ". LEFT", ":: LEFT")
    assign(150, "[ LEFT")
    assign(140, "+ RIGHT", "- RIGHT")
    assign(130, "^ LEFT")
    assign(120, "* LEFT", "/ LEFT", "% LEFT")
    assign(110, "+ LEFT", "- LEFT")
    # All other native and user-defined operators
    assign(100, "(any other operator) LEFT")
    assign(90, "BETWEEN NON", "IN NON", "LIKE NON", "ILIKE NON", "SIMILAR NON")
    assign(80, "< NON", "> NON", "= NON", "<= NON", ">= NON", "!= NON")
    assign(70, "IS NON", "ISNULL NON", "NOTNULL NON")
    assign(60, "NOT RIGHT")
    assign(50, "AND LEFT")
    assign(40, "OR LEFT")
    return mapping


# How the table above spells the operators that OPERATOR names otherwise. The
# rest are spelled in the table as their value is.
_TABLE_SPELLING: dict[OPERATOR, str] = {
    OPERATOR.NEG: "-",
    OPERATOR.IS_NULL: "ISNULL",
    OPERATOR.IS_NOT_NULL: "NOTNULL",
}

# How PostgreSQL spells the operators that OPERATOR names otherwise. The rest
# are spelled in a query as their value is.
#
# ``IS`` takes a keyword - TRUE, NULL - and not a parameter: ``x IS $1`` is a
# syntax error. ``IS NOT DISTINCT FROM`` is the same equality, in which null
# is a value, takes any expression, and binds as ``IS`` does.
_SQL_SPELLING: dict[OPERATOR, str] = {
    OPERATOR.NEG: "-",
    OPERATOR.IS: "IS NOT DISTINCT FROM",
}

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _quote(name: str) -> str:
    """
    Return the name between double quotes, a double quote of its own doubled.

    A word PostgreSQL knows is read as what PostgreSQL knows: `user` without
    quotes is the session's user, so `user = $1` parses and selects other
    rows than were asked for, and `order` does not parse. Which words these
    are depends on the server's version, which a library does not know; so
    no name is looked up in a list, and every name is quoted.

    Between quotes a name is the column's to the letter: `"createdAt"` is the
    column created as `"createdAt"`, which `createdAt` without quotes is not -
    PostgreSQL folds that to `createdat`. What a member of the domain is
    called in the storage is for the transform context to say.
    """
    return '"%s"' % name.replace('"', '""')


def _identifier(name: str) -> str:
    """
    Return a name as the query has it: checked, then quoted part by part.

    Two things keep SQL of a tree's own out of the text, and neither rests on
    the other: a name outside the alphabet is refused, and a double quote
    inside a name is doubled. Only values are parameters, and names used to
    be written as they were.

    Args:
        name: A name, or names joined with dots: `public.items`

    Raises:
        ValueError: If a part of the name is anything but letters, digits
            and "_", or starts with a digit
    """
    parts = name.split(".")
    for part in parts:
        if _IDENTIFIER.match(part) is None:
            raise ValueError("'%s' is not a valid identifier" % name)
    return ".".join(_quote(part) for part in parts)


# The operators a run of which can be regrouped without a change of its
# value, nulls included, so that ``a AND (b AND c)`` needs no parentheses.
# True of the logical connectives and of nothing else: ``a - (b - c)`` is not
# ``a - b - c``, and even ``+`` overflows and rounds one way and not the other.
_REGROUPING = frozenset((OPERATOR.AND, OPERATOR.OR))


class PostgresqlVisitor(Visitor[SqlFragment]):
    """
    Visitor that generates PostgreSQL SQL from specification AST.

    Functional: each ``visit_*`` returns ``(sql_fragment, params)``. No mutable
    accumulator state — instead, scoped state (outer precedence, wildcard
    context) is captured in immutable ``__slots__`` attributes and changed by
    constructing a sub-visitor via ``_at_precedence``/``_enter_wildcard``.
    Monotonic counters ($-placeholder index, wildcard alias counter) live in a
    shared ``_Counters`` container that crosses sub-visitor boundaries.

    Handles:
    - Field path rendering (e.g., "something.tenant_id")
    - Parameterized value placeholders ($1, $2, ...)
    - Operator precedence with automatic parenthesization
    - Prefix operators (NOT, unary +/-)
    - Infix operators (AND, OR, =, <, >, etc.)
    - Collection/Wildcard operators with embedded (unnest) and relational (EXISTS) modes
    """

    _PRECEDENCE_MAPPING: dict[str, int] = _build_precedence_mapping()

    __slots__ = (
        '_counters', '_schema',
        '_outer_precedence', '_outer_apart',
        '_in_wildcard', '_wildcard_alias', '_wildcard_path',
    )

    def __init__(
        self,
        placeholder_index: int = 0,
        schema: Optional[SchemaRegistry] = None,
        *,
        _counters: Optional[_Counters] = None,
        _outer_precedence: int = 0,
        _outer_apart: bool = False,
        _in_wildcard: bool = False,
        _wildcard_alias: str = "",
        _wildcard_path: tuple[str, ...] = (),
    ):
        if _counters is None:
            _counters = _Counters(placeholder_index=placeholder_index)
        self._counters = _counters
        self._schema = schema
        self._outer_precedence = _outer_precedence
        # Whether an operand as tight as the outer operator is parenthesised:
        # it is on the side the operator does not group to.
        self._outer_apart = _outer_apart
        self._in_wildcard = _in_wildcard
        self._wildcard_alias = _wildcard_alias
        # The names from the aggregate to the collection of the current item,
        # through the collections on the way: what a schema names it by.
        self._wildcard_path = _wildcard_path

    # --- Sub-visitor builders ---

    def _at_precedence(self, prec: int, apart: bool = False) -> 'PostgresqlVisitor':
        """Return a sub-visitor with the given outer precedence.

        Args:
            prec: The precedence of the operator the operand is of
            apart: Whether the operand is on the side the operator does not
                group to, where one as tight as the operator is parenthesised
        """
        return PostgresqlVisitor(
            schema=self._schema,
            _counters=self._counters,
            _outer_precedence=prec,
            _outer_apart=apart,
            _in_wildcard=self._in_wildcard,
            _wildcard_alias=self._wildcard_alias,
            _wildcard_path=self._wildcard_path,
        )

    def _enter_wildcard(
        self, alias: str, path: tuple[str, ...], prec: int = 0
    ) -> 'PostgresqlVisitor':
        """Return a sub-visitor scoped to a new wildcard context.

        Args:
            alias: What the current item is called in the query
            path: The names from the aggregate to the collection of the item
            prec: The precedence of the operator the predicate is an operand
                of: none in ``WHERE predicate``, AND in ``WHERE keys AND predicate``
        """
        return PostgresqlVisitor(
            schema=self._schema,
            _counters=self._counters,
            _outer_precedence=prec,
            _in_wildcard=True,
            _wildcard_alias=alias,
            _wildcard_path=path,
        )

    # --- Precedence helpers ---

    def _lookup_precedence(self, node: Operable) -> int:
        """Return the inner precedence for an operable node."""
        # The key is made of values. It used to be made of the members,
        # "%s %s" % (node.operator(), node.associativity()), which for members
        # of a str-and-Enum is "OPERATOR.AND ASSOCIATIVITY.LEFT_ASSOCIATIVE":
        # in no row of the table, so every operator had the precedence of
        # "any other operator" and no parenthesis was ever written.
        key = "%s %s" % (
            _TABLE_SPELLING.get(node.operator(), node.operator().value),
            node.associativity().value,
        )
        return self._PRECEDENCE_MAPPING.get(
            key,
            self._PRECEDENCE_MAPPING.get(
                "(any other operator) LEFT", self._outer_precedence
            ),
        )

    def _spell(self, operator: OPERATOR) -> str:
        """Return the operator as PostgreSQL spells it."""
        return _SQL_SPELLING.get(operator, operator.value)

    def _wrap_parens(self, inner_prec: int, sql: str) -> str:
        """Add parentheses if inner precedence is lower than current outer,
        or is the same on the side the outer operator does not group to."""
        if inner_prec < self._outer_precedence:
            return "(%s)" % sql
        if inner_prec == self._outer_precedence and self._outer_apart:
            return "(%s)" % sql
        return sql

    # --- Visit methods ---

    def visit_global_scope(self, node: GlobalScope) -> SqlFragment:
        """Visit global scope node — produces no SQL fragment."""
        return "", []

    def visit_object(self, node: Object) -> SqlFragment:
        """Visit object node — produces no SQL fragment."""
        return "", []

    def visit_item(self, node: Item) -> SqlFragment:
        """Visit item node — produces no SQL fragment (handled by visit_field)."""
        return "", []

    def visit_collection(self, node: Collection) -> SqlFragment:
        """
        Visit collection node (Wildcard).

        Two modes:
        1. Embedded (an array of a composite type): EXISTS (SELECT 1 FROM unnest(collection) AS item WHERE predicate)
        2. Relational (separate table): EXISTS (SELECT 1 FROM table AS item WHERE fk_conditions AND predicate)
        """
        collection_name = self._extract_collection_name(node)
        field_name = ".".join(self._extract_logical_path(node))

        if self._schema is not None and self._schema.is_relational(field_name):
            return self._visit_relational_collection(node, field_name, collection_name)
        return self._visit_embedded_collection(node, collection_name)

    def _visit_embedded_collection(
        self, node: Collection, collection_name: str
    ) -> SqlFragment:
        """Generate SQL for collections kept as an array of a composite type, using unnest."""
        collection_path = self._extract_collection_path(node)

        self._counters.wildcard_counter += 1
        alias = "%s_%d" % (collection_name.lower(), self._counters.wildcard_counter)

        sub = self._enter_wildcard(alias, self._extract_logical_path(node))
        predicate_sql, predicate_params = node.predicate().accept(sub)

        sql = "EXISTS (SELECT 1 FROM unnest(%s) AS %s WHERE %s)" % (
            collection_path, _identifier(alias), predicate_sql,
        )
        return sql, predicate_params

    def _visit_relational_collection(
        self,
        node: Collection,
        field_name: str,
        collection_name: str,
    ) -> SqlFragment:
        """Generate SQL for collections in separate tables."""
        assert self._schema is not None
        mapping = self._schema.get(field_name)
        if mapping is None:
            # Fallback to embedded if no mapping found
            return self._visit_embedded_collection(node, collection_name)

        self._counters.wildcard_counter += 1
        alias = mapping.alias if mapping.alias else collection_name.lower()
        alias = "%s_%d" % (alias, self._counters.wildcard_counter)
        # The alias goes on as it is: a name is quoted where it is written.
        alias_ref = _identifier(alias)

        # Determine parent reference BEFORE entering new wildcard context
        parent_ref = _identifier(self._get_parent_ref_for_relational(node))

        # The predicate is an operand of the AND after the keys: written as it
        # is, `fk AND p OR q` selects through `q` the rows of other parents.
        sub = self._enter_wildcard(
            alias,
            self._extract_logical_path(node),
            self._PRECEDENCE_MAPPING["AND LEFT"],
        )
        predicate_sql, predicate_params = node.predicate().accept(sub)

        # Generate FK conditions (supports composite keys)
        fk_parts = []
        for fk in mapping.foreign_keys:
            fk_parts.append(
                "%s.%s = %s.%s"
                % (alias_ref, _identifier(fk.child_column), parent_ref, _identifier(fk.parent_column))
            )
        fk_conditions = " AND ".join(fk_parts)

        sql = "EXISTS (SELECT 1 FROM %s AS %s WHERE %s AND %s)" % (
            _identifier(mapping.table), alias_ref, fk_conditions, predicate_sql,
        )
        return sql, predicate_params

    def _get_parent_ref_for_relational(self, node: Collection) -> str:
        """
        Return parent reference based on what the path to the collection starts at.

        Called BEFORE entering a new wildcard context to get the correct outer reference.
        """
        # If the collection is one of the current item (a nested wildcard), use
        # the outer wildcard alias. A collection of the candidate named inside
        # the predicate of another is joined to the root row: it used to be
        # joined to the enclosing item, whatever it was a collection of.
        if self._in_wildcard and self._is_item_reference(self._extract_root(node)):
            return self._wildcard_alias

        # Otherwise, use schema's parent reference.
        if self._schema is not None:
            return self._schema.get_parent_ref()

        return ""

    def _extract_field_name(self, node: Collection) -> str:
        """Extract the field name from collection's parent Object."""
        parent = node.parent()
        if not parent.is_root():
            return parent.name()
        return ""

    def _extract_root(self, node: Collection) -> EmptiableObject:
        """Extract what the path to the collection starts at: GlobalScope or Item."""
        parent = node.parent()
        while not parent.is_root():
            parent = parent.parent()
        return parent

    def _extract_logical_path(self, node: Collection) -> tuple[str, ...]:
        """
        Extract the names from the aggregate to the collection.

        What a schema names the collection by: `("Categories", "Items")` for
        the items of a category, `("Items",)` for the items of the store. The
        last name alone, `_extract_field_name`, does not tell the two apart.
        """
        parts: List[str] = []

        # Walk up the parent chain to collect path components
        parent = node.parent()
        while not parent.is_root():
            parts.insert(0, parent.name())
            parent = parent.parent()

        # A path from the current item goes on from the path to its collection
        if self._in_wildcard and self._is_item_reference(parent):
            return self._wildcard_path + tuple(parts)
        return tuple(parts)

    def _extract_collection_path(self, node: Collection) -> str:
        """Extract the SQL path to a collection from a CollectionNode."""
        parts: List[str] = []

        # Walk up the parent chain to collect path components
        parent = node.parent()
        while not parent.is_root():
            parts.insert(0, parent.name())
            parent = parent.parent()

        # If we're in a wildcard context and the root parent is Item(), prefix with current alias.
        # This handles nested wildcards: category_1.Items instead of just Items
        if self._in_wildcard and self._is_item_reference(parent):
            if parts:
                return _identifier(self._wildcard_alias) + "." + _identifier(".".join(parts))
            return _identifier(self._wildcard_alias)

        return _identifier(".".join(parts))

    def _extract_collection_name(self, node: Collection) -> str:
        """
        Extract the collection name for alias generation.

        e.g., "Items" -> "Item", "Categories" -> "Category"
        """
        parent = node.parent()
        if not parent.is_root():
            return inflection.singularize(parent.name())
        return "item"  # fallback

    def _is_item_reference(self, obj: EmptiableObject) -> bool:
        """Check if the object is Item() (current item in wildcard)."""
        return isinstance(obj, Item)

    def visit_field(self, node: Field) -> SqlFragment:
        """
        Visit field node and render as SQL field path.

        Handles both normal field access and item references in wildcard context.
        """
        path = extract_field_path(node)

        # What the path starts at, not what the field's immediate parent is:
        # the parent of `name` in `@.maker.name` is the object `maker`, so
        # the item's alias was dropped and `"maker"."name"` written - the
        # column of another table, if the query had one of that name.
        if self._in_wildcard and self._is_item_reference(extract_field_root(node)):
            # This is a field of the current item: item.Price, item.Active, etc.
            row = _identifier(self._wildcard_alias)
            return self._member_of_row(row, self._wildcard_path, path), []

        # An object of the candidate kept in a table of its own
        if len(path) > 1 and self._schema is not None and self._schema.is_relational(path[0]):
            row = _identifier(self._schema.get_parent_ref())
            return self._member_of_row(row, (), path), []

        # Normal field access: from the candidate the dots stay, a qualified
        # name - `"s"."price"` is the column `price` of `s`.
        return _identifier(".".join(path)), []

    def _member_of_row(self, row: str, logical: tuple[str, ...], names: list[str]) -> str:
        """
        Return the member at ``names`` of the row written ``row``.

        An object on the way to the member is looked up in the schema, as a
        collection is, by the names that lead to it. Kept in a table of its
        own, it is read through its key, by a subquery in the column's place:
        it has at most the one row the key names, and is null if there is
        none, as a member of a composite that is null is. Not mentioned, it
        is a composite kept in its row - a Value Object - and the parentheses
        are what makes it that: with dots alone PostgreSQL reads a schema, a
        table and a column, and there is no such table.

        Args:
            row: The row, as the query has it: an alias, or a composite
            logical: The names that lead to the row's object in the schema
            names: The names from the row to the member
        """
        if len(names) == 1:
            return "%s.%s" % (row, _identifier(names[0]))

        logical = logical + (names[0],)
        field_name = ".".join(logical)
        mapping = None
        if self._schema is not None and self._schema.is_relational(field_name):
            mapping = self._schema.get(field_name)
        if mapping is None:
            composite = "(%s.%s)" % (row, _identifier(names[0]))
            return self._member_of_row(composite, logical, names[1:])

        self._counters.wildcard_counter += 1
        alias = mapping.alias if mapping.alias else names[0].lower()
        alias_ref = _identifier("%s_%d" % (alias, self._counters.wildcard_counter))
        keys = " AND ".join(
            "%s.%s = %s.%s"
            % (alias_ref, _identifier(fk.child_column), row, _identifier(fk.parent_column))
            for fk in mapping.foreign_keys
        )
        member = self._member_of_row(alias_ref, logical, names[1:])
        return "(SELECT %s FROM %s AS %s WHERE %s)" % (
            member, _identifier(mapping.table), alias_ref, keys,
        )

    def visit_value(self, node: Value) -> SqlFragment:
        """
        Visit value node and produce a parameterized placeholder.
        """
        self._counters.placeholder_index += 1
        return "$%d" % self._counters.placeholder_index, [node.value()]

    def visit_prefix(self, node: Prefix) -> SqlFragment:
        """
        Visit prefix node (e.g., NOT, unary -).

        Handles precedence and renders operator before operand.
        """
        inner_prec = self._lookup_precedence(node)
        # `NOT NOT a` reads as it should; `--a` reads as a comment.
        sub = self._at_precedence(
            inner_prec, apart=node.operator() is OPERATOR.NEG,
        )
        op_sql, op_params = node.operand().accept(sub)

        # Unary - doesn't need space
        if node.operator() is OPERATOR.NEG:
            sql = "%s%s" % (self._spell(node.operator()), op_sql)
        else:
            sql = "%s %s" % (self._spell(node.operator()), op_sql)

        return self._wrap_parens(inner_prec, sql), op_params

    def visit_infix(self, node: Infix) -> SqlFragment:
        """
        Visit infix node (e.g., AND, OR, =, <, >).

        Handles precedence and renders: left operator right
        """
        inner_prec = self._lookup_precedence(node)
        # An operand as tight as the operator is parenthesised on the side the
        # operator does not group to: `a - (b - c)`, `(a = b) = c`.
        regroups = node.operator() in _REGROUPING
        left_sub = self._at_precedence(
            inner_prec,
            apart=not regroups and node.associativity() != ASSOCIATIVITY.LEFT_ASSOCIATIVE,
        )
        right_sub = self._at_precedence(
            inner_prec,
            apart=not regroups and node.associativity() != ASSOCIATIVITY.RIGHT_ASSOCIATIVE,
        )
        left_sql, left_params = node.left().accept(left_sub)
        right_sql, right_params = node.right().accept(right_sub)

        sql = "%s %s %s" % (left_sql, self._spell(node.operator()), right_sql)
        return self._wrap_parens(inner_prec, sql), left_params + right_params

    def visit_postfix(self, node: Postfix) -> SqlFragment:
        """
        Visit postfix node (e.g., IS NULL).

        Handles precedence and renders operand before operator.
        """
        inner_prec = self._lookup_precedence(node)
        sub = self._at_precedence(inner_prec, apart=True)
        op_sql, op_params = node.operand().accept(sub)

        sql = "%s %s" % (op_sql, self._spell(node.operator()))
        return self._wrap_parens(inner_prec, sql), op_params
