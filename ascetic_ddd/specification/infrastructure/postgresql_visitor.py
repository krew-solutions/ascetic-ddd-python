"""PostgreSQL visitor for generating SQL from specification AST."""
import dataclasses

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
)
from ascetic_ddd.specification.domain.constants import OPERATOR

from ascetic_ddd.specification.infrastructure.transform_visitor import ITransformContext, TransformVisitor
from ascetic_ddd.specification.infrastructure.schema import SchemaRegistry


SqlFragment = Tuple[str, List[Any]]


def compile_specification(
    context: ITransformContext, expression: Visitable
) -> SqlFragment:
    """
    Compile a domain specification to SQL.

    Args:
        context: Transform context for mapping domain to infrastructure
        expression: Domain specification expression

    Returns:
        Tuple of (sql_string, parameters)
    """
    # First, transform domain expression to infrastructure expression
    infrastructure_expr = expression.accept(TransformVisitor(context))

    # Then, generate SQL from infrastructure expression
    return infrastructure_expr.accept(PostgresqlVisitor())


def compile_to_sql(
    expression: Visitable,
    schema: Optional[SchemaRegistry] = None
) -> SqlFragment:
    """
    Compile AST directly to SQL without context transformation.

    Useful for generated code where AST is already in the right form.

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
        '_outer_precedence', '_in_wildcard', '_wildcard_alias',
    )

    def __init__(
        self,
        placeholder_index: int = 0,
        schema: Optional[SchemaRegistry] = None,
        *,
        _counters: Optional[_Counters] = None,
        _outer_precedence: int = 0,
        _in_wildcard: bool = False,
        _wildcard_alias: str = "",
    ):
        if _counters is None:
            _counters = _Counters(placeholder_index=placeholder_index)
        self._counters = _counters
        self._schema = schema
        self._outer_precedence = _outer_precedence
        self._in_wildcard = _in_wildcard
        self._wildcard_alias = _wildcard_alias

    # --- Sub-visitor builders ---

    def _at_precedence(self, prec: int) -> 'PostgresqlVisitor':
        """Return a sub-visitor with the given outer precedence."""
        return PostgresqlVisitor(
            schema=self._schema,
            _counters=self._counters,
            _outer_precedence=prec,
            _in_wildcard=self._in_wildcard,
            _wildcard_alias=self._wildcard_alias,
        )

    def _enter_wildcard(self, alias: str) -> 'PostgresqlVisitor':
        """Return a sub-visitor scoped to a new wildcard context."""
        return PostgresqlVisitor(
            schema=self._schema,
            _counters=self._counters,
            _outer_precedence=0,
            _in_wildcard=True,
            _wildcard_alias=alias,
        )

    # --- Precedence helpers ---

    def _lookup_precedence(self, node: Operable) -> int:
        """Return the inner precedence for an operable node."""
        key = "%s %s" % (node.operator(), node.associativity())
        return self._PRECEDENCE_MAPPING.get(
            key,
            self._PRECEDENCE_MAPPING.get(
                "(any other operator) LEFT", self._outer_precedence
            ),
        )

    def _wrap_parens(self, inner_prec: int, sql: str) -> str:
        """Add parentheses if inner precedence is lower than current outer."""
        if inner_prec < self._outer_precedence:
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
        1. Embedded (JSONB/array): EXISTS (SELECT 1 FROM unnest(collection) AS item WHERE predicate)
        2. Relational (separate table): EXISTS (SELECT 1 FROM table AS item WHERE fk_conditions AND predicate)
        """
        collection_name = self._extract_collection_name(node)
        field_name = self._extract_field_name(node)

        if self._schema is not None and self._schema.is_relational(field_name):
            return self._visit_relational_collection(node, field_name, collection_name)
        return self._visit_embedded_collection(node, collection_name)

    def _visit_embedded_collection(
        self, node: Collection, collection_name: str
    ) -> SqlFragment:
        """Generate SQL for JSONB/array collections using unnest."""
        collection_path = self._extract_collection_path(node)

        self._counters.wildcard_counter += 1
        alias = "%s_%d" % (collection_name.lower(), self._counters.wildcard_counter)

        sub = self._enter_wildcard(alias)
        predicate_sql, predicate_params = node.predicate().accept(sub)

        sql = "EXISTS (SELECT 1 FROM unnest(%s) AS %s WHERE %s)" % (
            collection_path, alias, predicate_sql,
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

        # Determine parent reference BEFORE entering new wildcard context
        parent_ref = self._get_parent_ref_for_relational()

        sub = self._enter_wildcard(alias)
        predicate_sql, predicate_params = node.predicate().accept(sub)

        # Generate FK conditions (supports composite keys)
        fk_parts = []
        for fk in mapping.foreign_keys:
            fk_parts.append(
                "%s.%s = %s.%s"
                % (alias, fk.child_column, parent_ref, fk.parent_column)
            )
        fk_conditions = " AND ".join(fk_parts)

        sql = "EXISTS (SELECT 1 FROM %s AS %s WHERE %s AND %s)" % (
            mapping.table, alias, fk_conditions, predicate_sql,
        )
        return sql, predicate_params

    def _get_parent_ref_for_relational(self) -> str:
        """
        Return parent reference based on current wildcard context.

        Called BEFORE entering a new wildcard context to get the correct outer reference.
        """
        # If we are inside a nested wildcard, use the outer wildcard alias.
        if self._in_wildcard and self._wildcard_alias:
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
                return self._wildcard_alias + "." + ".".join(parts)
            return self._wildcard_alias

        return ".".join(parts)

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
        if self._in_wildcard and self._is_item_reference(node.object()):
            # This is a field of the current item: item.Price, item.Active, etc.
            return "%s.%s" % (self._wildcard_alias, node.name()), []

        # Normal field access
        path = extract_field_path(node)
        return ".".join(path), []

    def visit_value(self, node: Value) -> SqlFragment:
        """
        Visit value node and produce a parameterized placeholder.
        """
        self._counters.placeholder_index += 1
        return "$%d" % self._counters.placeholder_index, [node.value()]

    def visit_prefix(self, node: Prefix) -> SqlFragment:
        """
        Visit prefix node (e.g., NOT, unary +/-).

        Handles precedence and renders operator before operand.
        """
        inner_prec = self._lookup_precedence(node)
        sub = self._at_precedence(inner_prec)
        op_sql, op_params = node.operand().accept(sub)

        # Unary +/- don't need space
        if node.operator() in (OPERATOR.POS, OPERATOR.NEG):
            sql = "%s%s" % (node.operator().value, op_sql)
        else:
            sql = "%s %s" % (node.operator().value, op_sql)

        return self._wrap_parens(inner_prec, sql), op_params

    def visit_infix(self, node: Infix) -> SqlFragment:
        """
        Visit infix node (e.g., AND, OR, =, <, >).

        Handles precedence and renders: left operator right
        """
        inner_prec = self._lookup_precedence(node)
        sub = self._at_precedence(inner_prec)
        left_sql, left_params = node.left().accept(sub)
        right_sql, right_params = node.right().accept(sub)

        sql = "%s %s %s" % (left_sql, node.operator().value, right_sql)
        return self._wrap_parens(inner_prec, sql), left_params + right_params

    def visit_postfix(self, node: Postfix) -> SqlFragment:
        """
        Visit postfix node (e.g., IS NULL).

        Handles precedence and renders operand before operator.
        """
        inner_prec = self._lookup_precedence(node)
        sub = self._at_precedence(inner_prec)
        op_sql, op_params = node.operand().accept(sub)

        sql = "%s %s" % (op_sql, node.operator().value)
        return self._wrap_parens(inner_prec, sql), op_params
