"""
JSONPath to Raw SQL compiler using jsonpath2 library.

Compiles JSONPath expressions into raw SQL queries for normalized relational databases.
No ORM or Query Builder dependencies - pure SQL generation.
"""
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum

from jsonpath2.nodes.root import RootNode
from jsonpath2.nodes.subscript import SubscriptNode
from jsonpath2.nodes.terminal import TerminalNode
from jsonpath2.subscripts.objectindex import ObjectIndexSubscript
from jsonpath2.subscripts.filter import FilterSubscript
from jsonpath2.subscripts.wildcard import WildcardSubscript
from jsonpath2.expressions.operator import (
    BinaryOperatorExpression,
    EqualBinaryOperatorExpression,
    AndVariadicOperatorExpression,
    OrVariadicOperatorExpression,
)
from jsonpath2.expressions.some import SomeExpression
from jsonpath2.nodes.current import CurrentNode
from jsonpath2.path import Path


class RelationType(Enum):
    """Type of relationship between tables."""
    ONE_TO_MANY = "one-to-many"
    MANY_TO_ONE = "many-to-one"
    MANY_TO_MANY = "many-to-many"


@dataclass
class ColumnDef:
    """Column definition."""
    name: str
    type: str  # SQL type: INTEGER, VARCHAR, etc
    nullable: bool = True
    primary_key: bool = False


@dataclass
class TableDef:
    """Table definition."""
    name: str
    columns: Dict[str, ColumnDef]  # column_name -> ColumnDef
    primary_key: str | Tuple[str, ...] = "id"  # Support composite PK

    def has_column(self, column_name: str) -> bool:
        """Check if table has column."""
        return column_name in self.columns

    def get_column_list(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())

    def get_primary_key_columns(self) -> List[str]:
        """Get primary key columns as list."""
        if isinstance(self.primary_key, str):
            return [self.primary_key]
        return list(self.primary_key)


@dataclass
class RelationshipDef:
    """Relationship definition."""
    target_table: str
    foreign_key: str | Tuple[str, ...]  # FK column(s) in source table, supports composite FK
    target_primary_key: str | Tuple[str, ...] = "id"  # Target PK column(s), supports composite PK
    relationship_type: RelationType = RelationType.ONE_TO_MANY

    def get_foreign_key_columns(self) -> List[str]:
        """Get foreign key columns as list."""
        if isinstance(self.foreign_key, str):
            return [self.foreign_key]
        return list(self.foreign_key)

    def get_target_primary_key_columns(self) -> List[str]:
        """Get target primary key columns as list."""
        if isinstance(self.target_primary_key, str):
            return [self.target_primary_key]
        return list(self.target_primary_key)


@dataclass
class SchemaDef:
    """Schema definition."""
    tables: Dict[str, TableDef]  # table_name -> TableDef
    relationships: Dict[str, Dict[str, RelationshipDef]]  # table -> {field -> relationship}
    root_table: str

    def get_table(self, table_name: str) -> TableDef:
        """Get table definition."""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema")
        return self.tables[table_name]


class SQLQuery:
    """Raw SQL query builder."""

    def __init__(self, root_table: str):
        """Initialize query with root table."""
        self.root_table = root_table
        self.select_columns: List[str] = []  # Qualified column names
        self.from_table = root_table
        self.joins: List[str] = []  # JOIN clauses
        self.where_conditions: List[str] = []  # WHERE conditions
        self.joined_tables: Set[str] = {root_table}

    def add_select_column(self, table: str, column: str):
        """Add column to SELECT."""
        qualified = f"{table}.{column}"
        if qualified not in self.select_columns:
            self.select_columns.append(qualified)

    def add_select_all(self, table: str):
        """Add all columns from table to SELECT."""
        qualified = f"{table}.*"
        if qualified not in self.select_columns:
            self.select_columns.append(qualified)

    def add_join(
        self,
        target_table: str,
        source_table: str,
        foreign_key: str | Tuple[str, ...],
        target_primary_key: str | Tuple[str, ...] = "id",
    ):
        """Add JOIN clause. Supports composite keys."""
        if target_table in self.joined_tables:
            return  # Already joined

        # Handle composite keys
        fk_columns = [foreign_key] if isinstance(foreign_key, str) else list(foreign_key)
        pk_columns = [target_primary_key] if isinstance(target_primary_key, str) else list(target_primary_key)

        if len(fk_columns) != len(pk_columns):
            raise ValueError(
                f"Foreign key and primary key must have same number of columns: "
                f"{len(fk_columns)} != {len(pk_columns)}"
            )

        # Build join conditions
        join_conditions = [
            f"{source_table}.{fk} = {target_table}.{pk}"
            for fk, pk in zip(fk_columns, pk_columns)
        ]

        join_clause = f"JOIN {target_table} ON {' AND '.join(join_conditions)}"
        self.joins.append(join_clause)
        self.joined_tables.add(target_table)

    def add_where(self, condition: str):
        """Add WHERE condition."""
        if condition:
            self.where_conditions.append(condition)

    def build(self) -> str:
        """Build final SQL query."""
        # SELECT clause
        if self.select_columns:
            select_clause = f"SELECT {', '.join(self.select_columns)}"
        else:
            select_clause = f"SELECT {self.from_table}.*"

        # FROM clause
        from_clause = f"FROM {self.from_table}"

        # JOIN clauses
        join_clause = "\n".join(self.joins) if self.joins else ""

        # WHERE clause
        where_clause = ""
        if self.where_conditions:
            where_clause = f"WHERE {' AND '.join(self.where_conditions)}"

        # Combine all parts
        parts = [select_clause, from_clause]
        if join_clause:
            parts.append(join_clause)
        if where_clause:
            parts.append(where_clause)

        return "\n".join(parts)


class CompilationContext:
    """Context for tracking compilation state."""

    def __init__(self, schema: SchemaDef):
        self.schema = schema
        self.current_table = schema.root_table
        self.query = SQLQuery(schema.root_table)

    def get_table(self, table_name: str) -> TableDef:
        """Get table definition."""
        return self.schema.get_table(table_name)

    def get_current_table(self) -> TableDef:
        """Get current table being processed."""
        return self.get_table(self.current_table)

    def get_relationship(self, field_name: str) -> Optional[RelationshipDef]:
        """Get relationship for field in current table."""
        if self.current_table not in self.schema.relationships:
            return None
        return self.schema.relationships[self.current_table].get(field_name)

    def add_join(self, target_table: str, relationship: RelationshipDef):
        """Add JOIN to target table. Supports composite keys and relationship types."""
        if target_table in self.query.joined_tables:
            return  # Already joined

        source_table = self.current_table

        # Handle composite keys
        fk = relationship.foreign_key
        pk = relationship.target_primary_key
        fk_columns = [fk] if isinstance(fk, str) else list(fk)
        pk_columns = [pk] if isinstance(pk, str) else list(pk)

        if len(fk_columns) != len(pk_columns):
            raise ValueError(
                f"Foreign key and primary key must have same number of columns: "
                f"{len(fk_columns)} != {len(pk_columns)}"
            )

        # Create join conditions based on relationship type
        # For ONE_TO_MANY: FK is in target table, PK is in source table
        # For MANY_TO_ONE: FK is in source table, PK is in target table
        if relationship.relationship_type == RelationType.ONE_TO_MANY:
            # FK columns in target table, PK columns in source table
            join_conds = [
                f"{source_table}.{pk} = {target_table}.{fk}"
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # MANY_TO_ONE or MANY_TO_MANY
            # FK columns in source table, PK columns in target table
            join_conds = [
                f"{source_table}.{fk} = {target_table}.{pk}"
                for fk, pk in zip(fk_columns, pk_columns)
            ]

        join_condition = " AND ".join(join_conds)
        self.query.joins.append(f"JOIN {target_table} ON {join_condition}")
        self.query.joined_tables.add(target_table)

    def add_where(self, condition: str):
        """Add WHERE condition."""
        self.query.add_where(condition)

    def add_select_column(self, column: str):
        """Add column to SELECT."""
        self.query.add_select_column(self.current_table, column)

    def add_select_all(self):
        """Add all columns from current table to SELECT."""
        self.query.add_select_all(self.current_table)


class NodeVisitor(ABC):
    """Abstract base visitor for AST nodes."""

    @abstractmethod
    def visit(self, node, context: CompilationContext):
        """Visit node and compile to SQL."""
        pass


class RootNodeVisitor(NodeVisitor):
    """Visitor for RootNode ($)."""

    def visit(self, node: RootNode, context: CompilationContext):
        """Visit root node - initialize from root table."""
        visitor = get_visitor(node.next_node)
        return visitor.visit(node.next_node, context)


class SubscriptNodeVisitor(NodeVisitor):
    """Visitor for SubscriptNode ([...])."""

    def visit(self, node: SubscriptNode, context: CompilationContext):
        """Visit subscript node - process each subscript."""
        for subscript in node.subscripts:
            subscript_visitor = get_subscript_visitor(subscript)
            subscript_visitor.visit(subscript, context)

        # Continue to next node
        if not isinstance(node.next_node, TerminalNode):
            visitor = get_visitor(node.next_node)
            return visitor.visit(node.next_node, context)


class TerminalNodeVisitor(NodeVisitor):
    """Visitor for TerminalNode (end of path)."""

    def visit(self, node: TerminalNode, context: CompilationContext):
        """Visit terminal node - compilation complete."""
        pass


class SubscriptVisitor(ABC):
    """Abstract base visitor for subscript nodes."""

    @abstractmethod
    def visit(self, subscript, context: CompilationContext):
        """Visit subscript and update context."""
        pass


class ObjectIndexSubscriptVisitor(SubscriptVisitor):
    """Visitor for ObjectIndexSubscript (.field or ['field'])."""

    def visit(self, subscript: ObjectIndexSubscript, context: CompilationContext):
        """Visit object index subscript."""
        field_name = subscript.index

        # Check if this is a relationship
        relationship = context.get_relationship(field_name)

        if relationship:
            # This is a relationship - add JOIN
            context.add_join(relationship.target_table, relationship)
            context.current_table = relationship.target_table
        else:
            # This is a regular column - add to SELECT
            current_table = context.get_current_table()
            if current_table.has_column(field_name):
                context.add_select_column(field_name)
            else:
                raise ValueError(
                    f"Column '{field_name}' not found in table '{context.current_table}'"
                )


class FilterSubscriptVisitor(SubscriptVisitor):
    """Visitor for FilterSubscript ([?(expression)])."""

    def visit(self, subscript: FilterSubscript, context: CompilationContext):
        """Visit filter subscript - compile to WHERE clause."""
        expression = subscript.expression
        condition = self._compile_expression(expression, context)
        context.add_where(condition)

    def _compile_expression(self, expression, context: CompilationContext) -> str:
        """Compile any expression type to SQL condition."""
        if isinstance(expression, SomeExpression):
            return self._compile_some_expression(expression, context)
        elif isinstance(expression, AndVariadicOperatorExpression):
            return self._compile_variadic_operator(expression, context, "AND")
        elif isinstance(expression, OrVariadicOperatorExpression):
            return self._compile_variadic_operator(expression, context, "OR")
        elif isinstance(expression, BinaryOperatorExpression):
            return self._compile_binary_operator(expression, context)
        else:
            raise NotImplementedError(f"Expression type {type(expression)} not supported")

    def _compile_variadic_operator(
        self, expr, context: CompilationContext, operator: str
    ) -> str:
        """Compile variadic operator expression (AND/OR) to SQL condition."""
        conditions = []
        for sub_expr in expr.expressions:
            condition = self._compile_expression(sub_expr, context)
            conditions.append(condition)
        return f"({f' {operator} '.join(conditions)})"

    def _compile_binary_operator(
        self, expr: BinaryOperatorExpression, context: CompilationContext
    ) -> str:
        """Compile binary operator expression to SQL condition."""
        # Get left operand
        if hasattr(expr.left_node_or_value, "__jsonpath__"):
            left_column = self._compile_node_to_column(expr.left_node_or_value, context)
        else:
            left_value = self._format_value(expr.left_node_or_value)
            left_column = left_value

        # Get right operand
        if hasattr(expr.right_node_or_value, "__jsonpath__"):
            right_column = self._compile_node_to_column(expr.right_node_or_value, context)
        else:
            right_value = self._format_value(expr.right_node_or_value)
            right_column = right_value

        # Compile operator
        operator_map = {
            "=": "=",
            "!=": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
        }

        operator = operator_map.get(expr.token, expr.token)
        return f"{left_column} {operator} {right_column}"

    def _compile_node_to_column(self, node, context: CompilationContext) -> str:
        """Compile a node reference (like @.field or @.orders.total) to qualified column name."""
        from jsonpath2.nodes.current import CurrentNode
        from jsonpath2.nodes.terminal import TerminalNode

        if isinstance(node, CurrentNode):
            # Collect all field names in the path
            field_names = []
            current = node.next_node

            while current is not None:
                if isinstance(current, SubscriptNode):
                    for subscript in current.subscripts:
                        if isinstance(subscript, ObjectIndexSubscript):
                            field_names.append(subscript.index)
                    current = current.next_node
                elif isinstance(current, TerminalNode):
                    break
                else:
                    break

            if not field_names:
                raise NotImplementedError("Empty path in filter expression")

            # Simple path: @.field
            if len(field_names) == 1:
                return f"{context.current_table}.{field_names[0]}"

            # Nested path: @.orders.total - need to add JOINs
            return self._compile_nested_path(field_names, context)

        raise NotImplementedError(f"Node type {type(node)} not supported in filter")

    def _compile_nested_path(self, field_names: list, context: CompilationContext) -> str:
        """
        Compile nested path to SQL with JOINs.

        Args:
            field_names: List of field names in the path (e.g., ['orders', 'total'])
            context: Compilation context

        Returns:
            Qualified column reference (e.g., "orders.total")
        """
        # Save current table to restore later
        saved_table = context.current_table

        # Navigate through relationships (all fields except the last)
        for field_name in field_names[:-1]:
            relationship = context.get_relationship(field_name)

            if not relationship:
                raise ValueError(
                    f"Field '{field_name}' is not a relationship in table '{context.current_table}'"
                )

            # Add JOIN for this relationship
            context.add_join(relationship.target_table, relationship)
            context.current_table = relationship.target_table

        # Get the final field name
        final_field = field_names[-1]
        target_table = context.current_table

        # Verify the field exists in the target table
        table_def = context.schema.get_table(target_table)
        if not table_def.has_column(final_field):
            raise ValueError(
                f"Column '{final_field}' not found in table '{target_table}'"
            )

        # Restore original table context
        context.current_table = saved_table

        # Return qualified column reference
        return f"{target_table}.{final_field}"

    def _format_value(self, value) -> str:
        """Format Python value to SQL literal."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")

    def _compile_some_expression(
        self, expr: SomeExpression, context: CompilationContext
    ) -> str:
        """
        Compile SomeExpression (nested wildcard) to EXISTS subquery.

        Pattern: @.items[*][?(@.price > 100)]
        Compiles to:
          EXISTS (
            SELECT 1 FROM items
            WHERE items.parent_id = parent.id
              AND items.price > 100
          )

        Args:
            expr: SomeExpression containing nested wildcard pattern
            context: Compilation context

        Returns:
            SQL EXISTS clause
        """
        # SomeExpression has next_node_or_value which is CurrentNode (@)
        current_node = expr.next_node_or_value
        if not isinstance(current_node, CurrentNode):
            raise NotImplementedError(
                f"SomeExpression with non-CurrentNode not supported: {type(current_node)}"
            )

        # Navigate through the path to find field name, wildcard, and inner filter
        # Structure: CurrentNode -> SubscriptNode[ObjectIndex] -> SubscriptNode[Wildcard] -> SubscriptNode[Filter]
        node = current_node.next_node
        if not isinstance(node, SubscriptNode):
            raise NotImplementedError("Expected SubscriptNode after CurrentNode")

        # Get field name from ObjectIndexSubscript
        field_name = None
        for subscript in node.subscripts:
            if isinstance(subscript, ObjectIndexSubscript):
                field_name = subscript.index
                break

        if not field_name:
            raise NotImplementedError("Could not find field name in SomeExpression")

        # Get relationship for the field
        relationship = context.get_relationship(field_name)
        if not relationship:
            raise ValueError(
                f"Field '{field_name}' is not a relationship in table '{context.current_table}'"
            )

        target_table = relationship.target_table
        source_table = context.current_table

        # Navigate to find the inner filter
        # Next should be SubscriptNode with WildcardSubscript
        node = node.next_node
        if not isinstance(node, SubscriptNode):
            raise NotImplementedError("Expected SubscriptNode with Wildcard")

        has_wildcard = any(isinstance(s, WildcardSubscript) for s in node.subscripts)
        if not has_wildcard:
            raise NotImplementedError("Expected WildcardSubscript in nested path")

        # Next should be SubscriptNode with FilterSubscript
        node = node.next_node
        if not isinstance(node, SubscriptNode):
            raise NotImplementedError("Expected SubscriptNode with Filter")

        inner_filter = None
        for subscript in node.subscripts:
            if isinstance(subscript, FilterSubscript):
                inner_filter = subscript
                break

        if not inner_filter:
            raise NotImplementedError("Could not find inner filter in SomeExpression")

        # Save current table and switch to target table context
        saved_table = context.current_table
        context.current_table = target_table

        # Compile inner filter expression in target table context
        filter_condition = self._compile_expression(inner_filter.expression, context)

        # Restore context
        context.current_table = saved_table

        # Build JOIN condition based on relationship type
        fk_columns = relationship.get_foreign_key_columns()
        pk_columns = relationship.get_target_primary_key_columns()

        if relationship.relationship_type == RelationType.ONE_TO_MANY:
            # FK is in target table, PK is in source table
            join_conditions = [
                f"{target_table}.{fk} = {source_table}.{pk}"
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # MANY_TO_ONE
            # FK is in source table, PK is in target table
            join_conditions = [
                f"{source_table}.{fk} = {target_table}.{pk}"
                for fk, pk in zip(fk_columns, pk_columns)
            ]

        join_condition = " AND ".join(join_conditions)

        # Build EXISTS subquery
        exists_query = (
            f"EXISTS (SELECT 1 FROM {target_table} "
            f"WHERE {join_condition} AND {filter_condition})"
        )

        return exists_query


class WildcardSubscriptVisitor(SubscriptVisitor):
    """Visitor for WildcardSubscript ([*])."""

    def visit(self, subscript: WildcardSubscript, context: CompilationContext):
        """Visit wildcard subscript - select all from current table."""
        context.add_select_all()


# Visitor registry
_node_visitors = {
    RootNode: RootNodeVisitor(),
    SubscriptNode: SubscriptNodeVisitor(),
    TerminalNode: TerminalNodeVisitor(),
}

_subscript_visitors = {
    ObjectIndexSubscript: ObjectIndexSubscriptVisitor(),
    FilterSubscript: FilterSubscriptVisitor(),
    WildcardSubscript: WildcardSubscriptVisitor(),
}


def get_visitor(node) -> NodeVisitor:
    """Get visitor for node type."""
    node_type = type(node)
    if node_type not in _node_visitors:
        raise NotImplementedError(f"No visitor for node type {node_type}")
    return _node_visitors[node_type]


def get_subscript_visitor(subscript) -> SubscriptVisitor:
    """Get visitor for subscript type."""
    subscript_type = type(subscript)
    if subscript_type not in _subscript_visitors:
        raise NotImplementedError(f"No visitor for subscript type {subscript_type}")
    return _subscript_visitors[subscript_type]


class JSONPathToRawSQLCompiler:
    """Compiler for JSONPath expressions to raw SQL queries."""

    def __init__(self, schema: SchemaDef):
        """Initialize compiler with schema definition."""
        self.schema = schema

    @staticmethod
    def _normalize_equality_operator(template: str) -> str:
        """
        Normalize == to = for jsonpath2 library compatibility.

        RFC 9535 standard defines == for equality, but jsonpath2 library
        deviates from the standard and uses single =.
        This method provides better UX by accepting both syntaxes.

        Args:
            template: JSONPath template string

        Returns:
            Normalized template with == replaced by =
        """
        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(template):
            char = template[i]

            # Track if we're inside a string literal
            if char in ('"', "'") and (i == 0 or template[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Replace == with = only outside strings
            if not in_string and char == '=' and i + 1 < len(template) and template[i + 1] == '=':
                result.append('=')
                i += 2  # Skip both = characters
                continue

            result.append(char)
            i += 1

        return ''.join(result)

    @staticmethod
    def _normalize_logical_operators(template: str) -> str:
        """
        Normalize RFC 9535 logical operators to jsonpath2 text operators.

        RFC 9535 standard defines: &&, ||, !
        jsonpath2 library uses text operators: and, or, not
        This method normalizes symbol operators to text operators.

        Args:
            template: JSONPath template string

        Returns:
            Normalized template with text logical operators
        """
        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(template):
            char = template[i]

            # Track if we're inside a string literal
            if char in ('"', "'") and (i == 0 or template[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                # Replace && with and
                if char == '&' and i + 1 < len(template) and template[i + 1] == '&':
                    result.append(' and ')
                    i += 2
                    continue

                # Replace || with or
                elif char == '|' and i + 1 < len(template) and template[i + 1] == '|':
                    result.append(' or ')
                    i += 2
                    continue

                # Replace ! with not (but not in !=)
                elif char == '!' and i + 1 < len(template) and template[i + 1] != '=':
                    result.append('not ')
                    i += 1
                    continue

            result.append(char)
            i += 1

        return ''.join(result)

    @staticmethod
    def _add_parentheses_to_filter(template: str) -> str:
        """
        Add parentheses around filter expressions if not present.

        jsonpath2 library requires parentheses: $[?(@.age > 25)] not $[?@.age > 25]
        RFC 9535 allows both syntaxes, so we normalize to jsonpath2 format.

        Args:
            template: JSONPath template string

        Returns:
            Template with parentheses added
        """
        result = template

        # Find all [?@ patterns that don't have ( after ?
        pattern = r'\[\?(?!\()'  # [? not followed by (
        positions = []
        for match in re.finditer(pattern, result):
            # Check if next char is @ or space then @
            pos = match.end()
            if pos < len(result) and (result[pos] == '@' or (result[pos] == ' ' and pos + 1 < len(result) and result[pos + 1] == '@')):
                positions.append(match.start())

        # Process from right to left to maintain positions
        for pos in reversed(positions):
            # Find the matching ]
            depth = 1
            i = pos + 2  # After [?
            closing_pos = None

            while i < len(result) and depth > 0:
                if result[i] == '[':
                    depth += 1
                elif result[i] == ']':
                    depth -= 1
                    if depth == 0:
                        closing_pos = i
                        break
                i += 1

            if closing_pos is not None:
                # Insert ) before ]
                result = result[:closing_pos] + ')' + result[closing_pos:]
                # Insert ( after ?
                insert_pos = pos + 2  # After [?
                result = result[:insert_pos] + '(' + result[insert_pos:]

        return result

    def compile(self, jsonpath_str: str) -> str:
        """
        Compile JSONPath expression to raw SQL query.

        Args:
            jsonpath_str: JSONPath expression string

        Returns:
            Raw SQL query string

        Example:
            >>> schema = SchemaDef(...)
            >>> compiler = JSONPathToRawSQLCompiler(schema)
            >>> sql = compiler.compile("$.users[?(@.age > 18)].orders")
            >>> print(sql)
            SELECT orders.*
            FROM users
            JOIN orders ON users.user_id = orders.id
            WHERE users.age > 18
        """
        # Add parentheses to filter expressions (required by jsonpath2)
        normalized_jsonpath = self._add_parentheses_to_filter(jsonpath_str)

        # Normalize == to = for jsonpath2 library compatibility
        normalized_jsonpath = self._normalize_equality_operator(normalized_jsonpath)

        # Normalize logical operators for jsonpath2 library compatibility
        normalized_jsonpath = self._normalize_logical_operators(normalized_jsonpath)

        # Parse JSONPath
        path = Path.parse_str(normalized_jsonpath)

        # Create compilation context
        context = CompilationContext(self.schema)

        # Visit AST and compile to SQL
        visitor = get_visitor(path.root_node)
        visitor.visit(path.root_node, context)

        # Build final SQL query
        return context.query.build()
