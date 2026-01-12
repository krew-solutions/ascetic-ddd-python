"""PostgreSQL visitor for generating SQL from specification AST."""
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..domain.nodes import (
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
    extract_field_path,
)
from ..domain.constants import OPERATOR

from .transform_visitor import ITransformContext, TransformVisitor


def compile_specification(
    context: ITransformContext, expression: Visitable
) -> Tuple[str, List[Any]]:
    """
    Compile a domain specification to SQL.

    Args:
        context: Transform context for mapping domain to infrastructure
        expression: Domain specification expression

    Returns:
        Tuple of (sql_string, parameters)
    """
    # First, transform domain expression to infrastructure expression
    transform_visitor = TransformVisitor(context)
    expression.accept(transform_visitor)
    infrastructure_expr = transform_visitor.result()

    # Then, generate SQL from infrastructure expression
    postgresql_visitor = PostgresqlVisitor()
    infrastructure_expr.accept(postgresql_visitor)
    return postgresql_visitor.result()


class PostgresqlVisitor(Visitor):
    """
    Visitor that generates PostgreSQL SQL from specification AST.

    Handles:
    - Field path rendering (e.g., "something.tenant_id")
    - Parameterized value placeholders ($1, $2, ...)
    - Operator precedence with automatic parenthesization
    - Prefix operators (NOT, unary +/-)
    - Infix operators (AND, OR, =, <, >, etc.)
    """

    def __init__(self, placeholder_index: int = 0):
        self._sql = ""
        self._placeholder_index = placeholder_index
        self._parameters: List[Any] = []
        self._precedence = 0
        self._precedence_mapping: Dict[str, int] = {}
        self._setup_precedence()

    def _setup_precedence(self) -> None:
        """
        Setup PostgreSQL operator precedence.

        Based on: https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-PRECEDENCE-TABLE
        """
        # Higher numbers = higher precedence
        self._set_precedence(160, ". LEFT", ":: LEFT")
        self._set_precedence(150, "[ LEFT")
        self._set_precedence(140, "+ RIGHT", "- RIGHT")
        self._set_precedence(130, "^ LEFT")
        self._set_precedence(120, "* LEFT", "/ LEFT", "% LEFT")
        self._set_precedence(110, "+ LEFT", "- LEFT")
        # All other native and user-defined operators
        self._set_precedence(100, "(any other operator) LEFT")
        self._set_precedence(
            90, "BETWEEN NON", "IN NON", "LIKE NON", "ILIKE NON", "SIMILAR NON"
        )
        self._set_precedence(
            80, "< NON", "> NON", "= NON", "<= NON", ">= NON", "!= NON"
        )
        self._set_precedence(70, "IS NON", "ISNULL NON", "NOTNULL NON")
        self._set_precedence(60, "NOT RIGHT")
        self._set_precedence(50, "AND LEFT")
        self._set_precedence(40, "OR LEFT")

    def _set_precedence(self, precedence: int, *operators: str) -> None:
        """Set precedence for given operators."""
        for op in operators:
            self._precedence_mapping[op] = precedence

    def _get_node_precedence_key(self, node: Operable) -> str:
        """Get precedence key for an operable node."""
        operator = node.operator()
        associativity = node.associativity()
        return f"{operator} {associativity}"

    def _visit(self, precedence_key: str, callable_fn: Callable[[], None]) -> None:
        """
        Visit with precedence handling.

        Automatically adds parentheses if inner precedence is lower than outer.
        """
        outer_precedence = self._precedence
        inner_precedence = self._precedence_mapping.get(
            precedence_key,
            self._precedence_mapping.get("(any other operator) LEFT", outer_precedence),
        )

        self._precedence = inner_precedence

        # Add opening parenthesis if needed
        if inner_precedence < outer_precedence:
            self._sql += "("

        callable_fn()

        # Add closing parenthesis if needed
        if inner_precedence < outer_precedence:
            self._sql += ")"

        self._precedence = outer_precedence

    def visit_global_scope(self, node: GlobalScope) -> None:
        """Visit global scope node."""
        pass

    def visit_object(self, node: Object) -> None:
        """Visit object node."""
        pass

    def visit_collection(self, node: Collection) -> None:
        """Visit collection node."""
        pass

    def visit_item(self, node: Item) -> None:
        """Visit item node."""
        pass

    def visit_field(self, node: Field) -> None:
        """
        Visit field node and render as SQL field path.

        Joins field path with dots (e.g., "something.tenant_id").
        """
        path = extract_field_path(node)
        name = ".".join(path)
        self._sql += name

    def visit_value(self, node: Value) -> None:
        """
        Visit value node and add parameterized placeholder.

        Adds parameter to list and renders as $N placeholder.
        """
        val = node.value()
        self._parameters.append(val)
        self._sql += f"${len(self._parameters)}"

    def visit_prefix(self, node: Prefix) -> None:
        """
        Visit prefix node (e.g., NOT, unary +/-).

        Handles precedence and renders operator before operand.
        """
        precedence_key = self._get_node_precedence_key(node)

        def visit_fn():
            operator = node.operator()
            # Unary +/- don't need space
            if operator in (OPERATOR.POS, OPERATOR.NEG):
                self._sql += str(operator.value)
            else:
                self._sql += f"{operator.value} "
            node.operand().accept(self)

        self._visit(precedence_key, visit_fn)

    def visit_infix(self, node: Infix) -> None:
        """
        Visit infix node (e.g., AND, OR, =, <, >).

        Handles precedence and renders: left operator right
        """
        precedence_key = self._get_node_precedence_key(node)

        def visit_fn():
            node.left().accept(self)
            self._sql += f" {node.operator().value} "
            node.right().accept(self)

        self._visit(precedence_key, visit_fn)

    def visit_postfix(self, node: Postfix) -> None:
        """
        Visit postfix node (e.g., IS NULL).

        Handles precedence and renders operator before operand.
        """
        precedence_key = self._get_node_precedence_key(node)

        def visit_fn():
            operator = node.operator()
            # Unary +/- don't need space
            if operator in (OPERATOR.POS, OPERATOR.NEG):
                self._sql += str(operator.value)
            else:
                self._sql += f"{operator.value} "
            node.operand().accept(self)

        self._visit(precedence_key, visit_fn)

    def result(self) -> Tuple[str, List[Any]]:
        """
        Return the generated SQL and parameters.

        Returns:
            Tuple of (sql_string, parameter_list)
        """
        return self._sql, self._parameters
