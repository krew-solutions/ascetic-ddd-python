"""PostgreSQL visitor for generating SQL from specification AST."""
import dataclasses
import datetime
import decimal
import re

import inflection
from typing import Any, List, NamedTuple, Optional, Tuple

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

from ascetic_ddd.specification.infrastructure.mapping_visitor import IMapping, transform
from ascetic_ddd.specification.infrastructure.schema import ForeignKey, SchemaRegistry


SqlFragment = Tuple[str, List[Any]]


def compile_specification(
    context: IMapping,
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
    called in the storage is for the mapping to say.
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


# The type PostgreSQL is told a constant has, where nothing else tells it.
#
# A constant is a numbered parameter, and the server finds its type from what
# stands beside it: `"age" >= $1` makes `$1` whatever `age` is. Where every
# operand of an operator is a constant there is nothing beside it: to a driver
# that asks the server for the types `$1 + $2` is "operator is not unique:
# unknown + unknown", and of `$1 IS NULL` the server "could not determine data
# type". psycopg sends a type with each value instead, and an integer's by its
# size - so the server computed `1 << 63` in sixteen bits and answered 0, and
# `30000 + 30000` was "smallint out of range".
#
# So there, and only there, the text says the type: `$1::bigint + $2::bigint`.
# Not everywhere. A value adapts to the column it meets, and a type said beside
# a column takes that away: `"at" = $1::timestamptz` of a column without zone
# is compared in the session's time zone, and selects other rows than
# `"at" = $1` does.
#
# The one column that is cast is the count of a shift, `"a" << "b"::integer`:
# PostgreSQL shifts by an `integer` and by nothing else, and a column there,
# a `bigint` more often than not, is "operator does not exist: bigint <<
# bigint". A cast of the count takes nothing away - the operator has it an
# integer already - and turns a column of any integer type into the one the
# operator has.
_ARITHMETIC = frozenset((
    OPERATOR.ADD, OPERATOR.SUB, OPERATOR.MUL, OPERATOR.DIV, OPERATOR.MOD,
    OPERATOR.LSHIFT, OPERATOR.RSHIFT,
))
_SHIFTS = frozenset((OPERATOR.LSHIFT, OPERATOR.RSHIFT))


def _param_type(value: Any) -> str:
    """
    Return what PostgreSQL calls the type of a value, by its kind.

    "" for None, which has none, and for a kind this does not know.
    """
    # A bool is an int to Python, and is not one here
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, decimal.Decimal):
        return "numeric"
    if isinstance(value, str):
        return "text"
    if isinstance(value, datetime.datetime):
        return "timestamptz"
    if isinstance(value, datetime.timedelta):
        return "interval"
    return ""


def _type_under_prefix(operator: OPERATOR, operand: Visitable) -> str:
    """
    Return the type to say of the operand of a prefix operator, if it is a constant.

    It has nothing beside it to take a type from. A null has no kind, and
    takes what the operator is of: a number under `-`; under NOT the server
    finds boolean by itself.
    """
    if not isinstance(operand, Value):
        return ""
    if operand.value() is None and operator is OPERATOR.NEG:
        return "bigint"
    return _param_type(operand.value())


def _type_under_postfix(operand: Visitable) -> str:
    """
    Return the type to say of the operand of a null test, if it is a constant.

    Of what type a null is tested does not matter, and the server must be
    told one: "could not determine data type of parameter".
    """
    if not isinstance(operand, Value):
        return ""
    if operand.value() is None:
        return "text"
    return _param_type(operand.value())


def _types_of_both(left: Visitable, operator: OPERATOR, right: Visitable) -> Tuple[str, str]:
    """
    Return the types to say of the operands of an operator, if both are constants.

    Neither has anything beside it to take a type from. PostgreSQL shifts a
    bigint by an integer, so the count of a shift is that. Two nulls take what
    the operator is of: numbers under arithmetic; compared, the server takes
    them for texts by itself.
    """
    if not isinstance(left, Value) or not isinstance(right, Value):
        return "", ""
    of_left, of_right = _param_type(left.value()), _param_type(right.value())
    if left.value() is None and right.value() is None and operator in _ARITHMETIC:
        of_left, of_right = "bigint", "bigint"
    if of_right == "bigint" and operator in _SHIFTS:
        of_right = "integer"
    return of_left, of_right


def _of_type(sql: str, said: str) -> str:
    """
    Return the text with its type said, if there is one to say.

    A cast binds tighter than any operator, so what was an atom is one still.
    """
    return "%s::%s" % (sql, said) if said else sql


def _is_a_count_to_cast(operator: OPERATOR, right: Visitable) -> bool:
    """
    Whether ``right`` is the count of a shift that must be said an integer.

    PostgreSQL shifts by an ``integer`` and by nothing else. A constant there
    is inferred, or was said an integer already where nothing stands beside
    it; a column or an expression has a type of its own, which the server
    will not convert, so it is cast.
    """
    return operator in _SHIFTS and not isinstance(right, Value)


# The operators a run of which can be regrouped without a change of its
# value, nulls included, so that ``a AND (b AND c)`` needs no parentheses.
# True of the logical connectives and of nothing else: ``a - (b - c)`` is not
# ``a - b - c``, and even ``+`` overflows and rounds one way and not the other.
_REGROUPING = frozenset((OPERATOR.AND, OPERATOR.OR))


def _singular_of(table: str) -> str:
    """Return the singular of the last name of ``table``, in lower case: what
    an alias is made of."""
    return inflection.singularize(table.rsplit(".", 1)[-1]).lower()


class _Wildcard(NamedTuple):
    """A collection whose predicate is being compiled: what its item's row
    is called in the query, and what that row is a row of to the schema - a
    table, or the composite at a column of one, ``stores.items``.
    """
    alias: str
    row: str


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
        '_wildcards',
    )

    def __init__(
        self,
        placeholder_index: int = 0,
        schema: Optional[SchemaRegistry] = None,
        *,
        _counters: Optional[_Counters] = None,
        _outer_precedence: int = 0,
        _outer_apart: bool = False,
        _wildcards: tuple[_Wildcard, ...] = (),
    ):
        if _counters is None:
            _counters = _Counters(placeholder_index=placeholder_index)
        self._counters = _counters
        self._schema = schema
        self._outer_precedence = _outer_precedence
        # Whether an operand as tight as the outer operator is parenthesised:
        # it is on the side the operator does not group to.
        self._outer_apart = _outer_apart
        # The collection whose item is under test, last, and before it the
        # enclosing ones: Item(depth) is the item of the one depth steps out.
        self._wildcards = _wildcards

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
            _wildcards=self._wildcards,
        )

    @property
    def _in_wildcard(self) -> bool:
        """Whether this is the predicate of a collection."""
        return bool(self._wildcards)

    def _wildcard(self, item: Item) -> "_Wildcard":
        """Return the collection whose item ``item`` is: the one its depth
        steps out.

        Raises:
            ValueError: If there is no collection that far out
        """
        if item.depth() >= len(self._wildcards):
            raise ValueError("No current item in context: the item %d collections out" % item.depth())
        return self._wildcards[-1 - item.depth()]

    def _candidates_column(self, name: str) -> str:
        """Return the column ``name`` of the candidate's row, inside the
        predicate of a collection.

        Unqualified, PostgreSQL reads it from the innermost row that has a
        column of that name, and a category with a ``limit`` of its own hid
        the shop's. The row is what the schema calls it.

        Raises:
            ValueError: Without a schema
        """
        if self._schema is None:
            raise ValueError(
                "A member of the candidate inside a collection's predicate needs"
                " the candidate's table: compile with a schema"
            )
        return _identifier(self._schema.row()) + "." + _identifier(name)

    def _enter_wildcard(self, alias: str, row: str, prec: int = 0) -> 'PostgresqlVisitor':
        """Return a sub-visitor scoped to a new wildcard context.

        Args:
            alias: What the current item is called in the query
            row: What the item is a row of, to the schema
            prec: The precedence of the operator the predicate is an operand
                of: none in ``WHERE predicate``, AND in ``WHERE keys AND predicate``
        """
        return PostgresqlVisitor(
            schema=self._schema,
            _counters=self._counters,
            _outer_precedence=prec,
            _wildcards=self._wildcards + (_Wildcard(alias, row),),
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
        # The row the collection is a name of, to the schema: the enclosing
        # item's, or the root's. Without a schema there is no relation to
        # look for, and the name is an array in the row.
        of = self._row_of(node)
        name = ".".join(self._extract_names(node))
        key = None if of is None else self._key_of_collection(of, name)

        if key is not None:
            return self._visit_relational_collection(node, key)
        return self._visit_embedded_collection(node, "%s.%s" % (of or "", name), collection_name)

    def _key_of_collection(self, row: str, name: str) -> Optional[ForeignKey]:
        """Return the key a collection named ``name`` in a row of ``row`` is
        joined by: the one of that name, if it references the row; else the
        one key on the table ``name`` that does. None: the name is an array
        in the row.

        Raises:
            ValueError: If the key named does not reference the row, or the
                table has several keys to it
        """
        if self._schema is None:
            return None
        named = self._schema.key_named(name)
        if named is not None:
            if named.referenced_table != row:
                raise ValueError("The key %s references %s, not %s" % (name, named.referenced_table, row))
            return named
        keys = self._schema.keys_referencing(name, row)
        if len(keys) > 1:
            raise ValueError(
                "%s has %d keys to %s: %s; name the key"
                % (name, len(keys), row, ", ".join(key.name for key in keys))
            )
        return keys[0] if keys else None

    def _key_of_object(self, row: str, name: str) -> Optional[ForeignKey]:
        """Return the key an object named ``name`` in a row of ``row`` is
        read through: the one of that name, if it is on the row; else the
        one key on the row that ``name`` is a column of. None: the name is a
        composite in the row.

        Raises:
            ValueError: If the key named is not on the row, or the column is
                of several keys
        """
        if self._schema is None:
            return None
        named = self._schema.key_named(name)
        if named is not None:
            if named.table != row:
                raise ValueError("The key %s is on %s, not %s" % (name, named.table, row))
            return named
        keys = self._schema.keys_on(row, name)
        if len(keys) > 1:
            raise ValueError(
                "%s is a column of %d keys of %s: %s; name the key"
                % (name, len(keys), row, ", ".join(key.name for key in keys))
            )
        return keys[0] if keys else None

    def _row_of(self, node: Collection) -> Optional[str]:
        """Return what the row a collection is a name of is a row of, to the schema."""
        root = self._extract_root(node)
        if isinstance(root, Item):
            return self._wildcard(root).row
        if self._schema is not None:
            return self._schema.table
        return None

    def _visit_embedded_collection(
        self, node: Collection, row: str, collection_name: str
    ) -> SqlFragment:
        """Generate SQL for collections kept as an array of a composite type, using unnest."""
        collection_path = self._extract_collection_path(node)

        self._counters.wildcard_counter += 1
        alias = "%s_%d" % (collection_name.lower(), self._counters.wildcard_counter)

        sub = self._enter_wildcard(alias, row)
        predicate_sql, predicate_params = node.predicate().accept(sub)

        sql = "EXISTS (SELECT 1 FROM unnest(%s) AS %s WHERE %s)" % (
            collection_path, _identifier(alias), predicate_sql,
        )
        return sql, predicate_params

    def _visit_relational_collection(self, node: Collection, key: ForeignKey) -> SqlFragment:
        """Generate SQL for collections in separate tables."""
        self._counters.wildcard_counter += 1
        # The alias is the compiler's own: the singular of the row's table,
        # numbered.
        alias = "%s_%d" % (_singular_of(key.table), self._counters.wildcard_counter)
        # The alias goes on as it is: a name is quoted where it is written.
        alias_ref = _identifier(alias)

        # Determine parent reference BEFORE entering new wildcard context
        parent_ref = _identifier(self._row_joined_to(node))

        # The predicate is an operand of the AND after the keys: written as it
        # is, `fk AND p OR q` selects through `q` the rows of other parents.
        sub = self._enter_wildcard(alias, key.table, self._PRECEDENCE_MAPPING["AND LEFT"])
        predicate_sql, predicate_params = node.predicate().accept(sub)

        # Generate FK conditions (supports composite keys)
        fk_parts = []
        for column, referenced in zip(key.columns, key.referenced_columns):
            fk_parts.append(
                "%s.%s = %s.%s"
                % (alias_ref, _identifier(column), parent_ref, _identifier(referenced))
            )
        fk_conditions = " AND ".join(fk_parts)

        sql = "EXISTS (SELECT 1 FROM %s AS %s WHERE %s AND %s)" % (
            _identifier(key.table), alias_ref, fk_conditions, predicate_sql,
        )
        return sql, predicate_params

    def _row_joined_to(self, node: Collection) -> str:
        """
        Return what the query calls the row a collection is joined to: the
        enclosing item's alias if the path to the collection starts at the
        item, else the query's own table.

        Called BEFORE entering a new wildcard context to get the correct outer reference.
        """
        # If the collection is one of the current item (a nested wildcard), use
        # the outer wildcard alias. A collection of the candidate named inside
        # the predicate of another is joined to the root row: it used to be
        # joined to the enclosing item, whatever it was a collection of.
        root = self._extract_root(node)
        if isinstance(root, Item):
            return self._wildcard(root).alias

        # Otherwise, use schema's parent reference.
        if self._schema is not None:
            return self._schema.row()

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

    def _extract_names(self, node: Collection) -> tuple[str, ...]:
        """Extract the names from what the path to the collection starts at."""
        parts: List[str] = []
        parent = node.parent()
        while not parent.is_root():
            parts.insert(0, parent.name())
            parent = parent.parent()
        return tuple(parts)

    def _extract_collection_path(self, node: Collection) -> str:
        """Extract the SQL path to a collection from a CollectionNode."""
        parts: List[str] = []

        # Walk up the parent chain to collect path components
        parent = node.parent()
        while not parent.is_root():
            parts.insert(0, parent.name())
            parent = parent.parent()

        # A collection of an item is under the alias of that item's row.
        # This handles nested wildcards: category_1.Items instead of just Items
        if isinstance(parent, Item):
            alias = _identifier(self._wildcard(parent).alias)
            if parts:
                return alias + "." + _identifier(".".join(parts))
            return alias

        if self._in_wildcard and len(parts) == 1 and "." not in parts[0]:
            return self._candidates_column(parts[0])
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
        root = extract_field_root(node)
        if isinstance(root, Item):
            # This is a field of an item: item.Price, item.Active, etc.
            wildcard = self._wildcard(root)
            return self._member_of_row(_identifier(wildcard.alias), wildcard.row, path), []

        # An object of the candidate kept in a table of its own, or a
        # composite column of its row - a Value Object - that the schema
        # says is one: the dots of an undeclared name are a qualifier.
        if (
            len(path) > 1 and self._schema is not None
            and (
                self._key_of_object(self._schema.table, path[0]) is not None
                or self._schema.is_composite(self._schema.table, path[0])
            )
        ):
            row = _identifier(self._schema.row())
            return self._member_of_row(row, self._schema.table, path), []

        # Inside a collection's predicate the candidate's column is qualified
        # with its row; a name of several parts the author qualified.
        if self._in_wildcard and len(path) == 1 and "." not in path[0]:
            return self._candidates_column(path[0]), []

        # Normal field access: from the candidate the dots stay, a qualified
        # name - `"s"."price"` is the column `price` of `s`.
        return _identifier(".".join(path)), []

    def _member_of_row(self, row: str, of: str, names: list[str]) -> str:
        """
        Return the member at ``names`` of the row written ``row``.

        An object on the way to the member is looked up in the schema, as a
        collection is, by the row it is a name of. Kept in a table of its
        own, it is read through its key, by a subquery in the column's place:
        it has at most the one row the key names, and is null if there is
        none, as a member of a composite that is null is. Not mentioned, it
        is a composite kept in its row - a Value Object - and the parentheses
        are what makes it that: with dots alone PostgreSQL reads a schema, a
        table and a column, and there is no such table.

        Args:
            row: The row, as the query has it: an alias, or a composite
            of: What the row is a row of, to the schema: a table, or the
                composite at a column of one
            names: The names from the row to the member
        """
        if len(names) == 1:
            return "%s.%s" % (row, _identifier(names[0]))

        key = self._key_of_object(of, names[0])
        if key is None:
            composite = "(%s.%s)" % (row, _identifier(names[0]))
            return self._member_of_row(composite, "%s.%s" % (of, names[0]), names[1:])

        # The row read is one of the referenced table, and its alias says so.
        self._counters.wildcard_counter += 1
        alias_ref = _identifier(
            "%s_%d" % (_singular_of(key.referenced_table), self._counters.wildcard_counter)
        )
        keys = " AND ".join(
            "%s.%s = %s.%s" % (alias_ref, _identifier(referenced), row, _identifier(column))
            for column, referenced in zip(key.columns, key.referenced_columns)
        )
        member = self._member_of_row(alias_ref, key.referenced_table, names[1:])
        return "(SELECT %s FROM %s AS %s WHERE %s)" % (
            member, _identifier(key.referenced_table), alias_ref, keys,
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
        op_sql = _of_type(op_sql, _type_under_prefix(node.operator(), node.operand()))

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
        of_left, of_right = _types_of_both(node.left(), node.operator(), node.right())
        if _is_a_count_to_cast(node.operator(), node.right()):
            # A cast binds tighter than any operator: the count is compiled
            # under the cast's precedence, so what is not an atom is
            # parenthesised, `("b" + $1)::integer`.
            right_sub = self._at_precedence(self._PRECEDENCE_MAPPING[":: LEFT"])
            of_right = "integer"
        left_sql, left_params = node.left().accept(left_sub)
        right_sql, right_params = node.right().accept(right_sub)
        left_sql, right_sql = _of_type(left_sql, of_left), _of_type(right_sql, of_right)

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
        op_sql = _of_type(op_sql, _type_under_postfix(node.operand()))

        sql = "%s %s" % (op_sql, self._spell(node.operator()))
        return self._wrap_parens(inner_prec, sql), op_params
