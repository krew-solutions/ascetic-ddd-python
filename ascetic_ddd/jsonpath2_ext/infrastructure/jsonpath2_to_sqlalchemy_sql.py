"""
JSONPath to SQL compiler using jsonpath2 library.

Compiles JSONPath expressions into SQL queries for normalized relational databases.
"""
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import Column, Table, and_, or_, exists
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.elements import BinaryExpression

from jsonpath2.nodes.root import RootNode
from jsonpath2.nodes.subscript import SubscriptNode
from jsonpath2.nodes.terminal import TerminalNode
from jsonpath2.nodes.current import CurrentNode
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
from jsonpath2.path import Path


@dataclass
class RelationshipMetadata:
    """Metadata about relationship between tables."""

    target_table: str  # Target table name
    foreign_key: str | Tuple[str, ...]  # Foreign key column(s) in SOURCE table, supports composite FK
    target_primary_key: str | Tuple[str, ...] = "id"  # Primary key column(s) in TARGET table, supports composite PK
    relationship_type: str = "one-to-many"  # Type: one-to-many, many-to-one, many-to-many

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
class SchemaMetadata:
    """Schema metadata for SQL compilation."""

    tables: Dict[str, Table]  # table_name -> SQLAlchemy Table object
    relationships: Dict[str, Dict[str, RelationshipMetadata]]  # table -> {field -> relationship}
    root_table: str  # Root table name (starting point)


class CompilationContext:
    """Context for tracking compilation state."""

    def __init__(self, schema: SchemaMetadata):
        self.schema = schema
        self.current_table = schema.root_table
        self.joined_tables: List[str] = [schema.root_table]  # Ordered list to preserve join order
        self.join_conditions: List[BinaryExpression] = []
        self.where_conditions: List[BinaryExpression] = []
        self.select_columns: List[Column] = []
        self.alias_counter = 0

    def get_table(self, table_name: str) -> Table:
        """Get SQLAlchemy Table by name."""
        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' not found in schema")
        return self.schema.tables[table_name]

    def get_current_table(self) -> Table:
        """Get current table being processed."""
        return self.get_table(self.current_table)

    def get_relationship(self, field_name: str) -> Optional[RelationshipMetadata]:
        """Get relationship metadata for field in current table."""
        if self.current_table not in self.schema.relationships:
            return None
        return self.schema.relationships[self.current_table].get(field_name)

    def add_join(self, target_table: str, relationship: RelationshipMetadata):
        """Add JOIN to target table. Supports composite keys and relationship types."""
        if target_table in self.joined_tables:
            return  # Already joined

        source_table = self.get_current_table()
        target_table_obj = self.get_table(target_table)

        # Handle composite keys
        fk_columns = relationship.get_foreign_key_columns()
        pk_columns = relationship.get_target_primary_key_columns()

        if len(fk_columns) != len(pk_columns):
            raise ValueError(
                f"Foreign key and primary key must have same number of columns: "
                f"{len(fk_columns)} != {len(pk_columns)}"
            )

        # Create join conditions based on relationship type
        # For ONE_TO_MANY: FK is in target table, PK is in source table
        # For MANY_TO_ONE: FK is in source table, PK is in target table
        if relationship.relationship_type == "one-to-many":
            # FK columns in target table, PK columns in source table
            join_conditions = [
                source_table.c[pk] == target_table_obj.c[fk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # many-to-one or many-to-many
            # FK columns in source table, PK columns in target table
            join_conditions = [
                source_table.c[fk] == target_table_obj.c[pk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]

        # Combine multiple conditions with AND
        if len(join_conditions) == 1:
            join_condition = join_conditions[0]
        else:
            join_condition = and_(*join_conditions)

        self.join_conditions.append(join_condition)
        self.joined_tables.append(target_table)

    def add_where(self, condition: BinaryExpression):
        """Add WHERE condition."""
        self.where_conditions.append(condition)


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
        # Root node just delegates to next node
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
        """
        Visit object index subscript.

        This represents accessing a field, which could be:
        1. A column in the current table
        2. A relationship to another table (requires JOIN)
        """
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
            if field_name in current_table.c:
                context.select_columns.append(current_table.c[field_name])
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

    def _compile_expression(self, expression, context: CompilationContext):
        """Compile any expression type to SQLAlchemy condition."""
        if isinstance(expression, SomeExpression):
            return self._compile_some_expression(expression, context)
        elif isinstance(expression, AndVariadicOperatorExpression):
            return self._compile_variadic_operator(expression, context, and_)
        elif isinstance(expression, OrVariadicOperatorExpression):
            return self._compile_variadic_operator(expression, context, or_)
        elif isinstance(expression, BinaryOperatorExpression):
            return self._compile_binary_operator(expression, context)
        else:
            raise NotImplementedError(f"Expression type {type(expression)} not supported")

    def _compile_variadic_operator(
        self, expr, context: CompilationContext, operator_func
    ):
        """Compile variadic operator expression (AND/OR) to SQLAlchemy condition."""
        conditions = []
        for sub_expr in expr.expressions:
            condition = self._compile_expression(sub_expr, context)
            conditions.append(condition)
        return operator_func(*conditions)

    def _compile_binary_operator(
        self, expr: BinaryOperatorExpression, context: CompilationContext
    ) -> BinaryExpression:
        """Compile binary operator expression to SQLAlchemy condition."""
        current_table = context.get_current_table()

        # Get left operand
        if hasattr(expr.left_node_or_value, "__jsonpath__"):
            # This is a node (e.g., @.age)
            left_operand = self._compile_node_to_column(expr.left_node_or_value, context)
        else:
            # This is a literal value - convert to column reference
            left_operand = current_table.c[expr.left_node_or_value]

        # Get right operand
        if hasattr(expr.right_node_or_value, "__jsonpath__"):
            right_operand = self._compile_node_to_column(expr.right_node_or_value, context)
        else:
            # This is a literal value
            right_operand = expr.right_node_or_value

        # Compile operator
        if isinstance(expr, EqualBinaryOperatorExpression):
            return left_operand == right_operand
        elif expr.token == "!=":
            return left_operand != right_operand
        elif expr.token == ">":
            return left_operand > right_operand
        elif expr.token == "<":
            return left_operand < right_operand
        elif expr.token == ">=":
            return left_operand >= right_operand
        elif expr.token == "<=":
            return left_operand <= right_operand
        else:
            raise NotImplementedError(f"Operator '{expr.token}' not supported")

    def _compile_node_to_column(self, node, context: CompilationContext) -> Column:
        """Compile a node reference (like @.field or @.orders.total) to a Column."""
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
                current_table = context.get_current_table()
                return current_table.c[field_names[0]]

            # Nested path: @.orders.total - need to add JOINs
            return self._compile_nested_path(field_names, context)

        raise NotImplementedError(f"Node type {type(node)} not supported in filter")

    def _compile_nested_path(self, field_names: list, context: CompilationContext) -> Column:
        """
        Compile nested path to Column with JOINs.

        Args:
            field_names: List of field names in the path (e.g., ['orders', 'total'])
            context: Compilation context

        Returns:
            Column reference from the final joined table
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

        # Get the final field name and target table
        final_field = field_names[-1]
        target_table = context.get_current_table()

        # Verify the field exists in the target table
        if final_field not in target_table.c:
            raise ValueError(
                f"Column '{final_field}' not found in table '{context.current_table}'"
            )

        # Restore original table context
        context.current_table = saved_table

        # Return column reference
        return target_table.c[final_field]

    def _compile_some_expression(
        self, expr: SomeExpression, context: CompilationContext
    ):
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
            SQLAlchemy EXISTS clause
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

        target_table_name = relationship.target_table
        source_table = context.get_current_table()
        target_table = context.get_table(target_table_name)

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
        context.current_table = target_table_name

        # Compile inner filter expression in target table context
        filter_condition = self._compile_expression(inner_filter.expression, context)

        # Restore context
        context.current_table = saved_table

        # Build JOIN condition based on relationship type
        fk_columns = relationship.get_foreign_key_columns()
        pk_columns = relationship.get_target_primary_key_columns()

        if relationship.relationship_type == "one-to-many":
            # FK is in target table, PK is in source table
            join_conditions = [
                target_table.c[fk] == source_table.c[pk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # many-to-one
            # FK is in source table, PK is in target table
            join_conditions = [
                source_table.c[fk] == target_table.c[pk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]

        # Combine join conditions
        if len(join_conditions) == 1:
            join_condition = join_conditions[0]
        else:
            join_condition = and_(*join_conditions)

        # Build EXISTS subquery
        exists_subquery = exists(
            select(1).select_from(target_table).where(
                and_(join_condition, filter_condition)
            )
        )

        return exists_subquery


class WildcardSubscriptVisitor(SubscriptVisitor):
    """Visitor for WildcardSubscript ([*])."""

    def visit(self, subscript: WildcardSubscript, context: CompilationContext):
        """Visit wildcard subscript - select all from current table."""
        current_table = context.get_current_table()
        # Add all columns from current table
        for column in current_table.c.values():
            if column not in context.select_columns:
                context.select_columns.append(column)


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


class JSONPathToSQLCompiler:
    """Compiler for JSONPath expressions to SQL queries."""

    def __init__(self, schema: SchemaMetadata):
        """Initialize compiler with schema metadata."""
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

    def compile(self, jsonpath_str: str) -> Select:
        """
        Compile JSONPath expression to SQLAlchemy Select query.

        Args:
            jsonpath_str: JSONPath expression string

        Returns:
            SQLAlchemy Select query object

        Example:
            >>> schema = SchemaMetadata(...)
            >>> compiler = JSONPathToSQLCompiler(schema)
            >>> query = compiler.compile("$.users[?(@.age > 18)].orders")
            >>> print(query)
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

        # Build SELECT query
        root_table = context.get_table(self.schema.root_table)

        # Start with root table - always use select_from to ensure correct FROM clause
        if context.select_columns:
            query = select(*context.select_columns).select_from(root_table)
        else:
            query = select(root_table)

        # Add JOINs - process in order to maintain proper join chain
        for joined_table_name in context.joined_tables:
            if joined_table_name == self.schema.root_table:
                continue
            joined_table = context.get_table(joined_table_name)
            # Find the join condition for this table
            for condition in context.join_conditions:
                # Check if condition references this table
                if str(joined_table.name) in str(condition):
                    query = query.join(joined_table, condition)
                    break

        # Add WHERE conditions
        if context.where_conditions:
            query = query.where(and_(*context.where_conditions))

        return query
