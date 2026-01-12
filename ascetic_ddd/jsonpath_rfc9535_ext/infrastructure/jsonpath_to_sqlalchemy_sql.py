"""
JSONPath to SQLAlchemy SQL compiler using jsonpath-rfc9535 library (RFC 9535 compliant).

Compiles JSONPath expressions into SQLAlchemy Select queries for normalized relational databases.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum

try:
    from sqlalchemy import Column, Table, and_, or_, not_, exists
    from sqlalchemy.sql import Select, select
    from sqlalchemy.sql.elements import BinaryExpression
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    Column = None
    Table = None
    Select = None
    exists = None

from jsonpath_rfc9535 import JSONPathEnvironment
from jsonpath_rfc9535.selectors import NameSelector, WildcardSelector, FilterSelector
from jsonpath_rfc9535.filter_expressions import ComparisonExpression, LogicalExpression


class RelationType(Enum):
    """Type of relationship between tables."""
    ONE_TO_MANY = "one-to-many"
    MANY_TO_ONE = "many-to-one"
    MANY_TO_MANY = "many-to-many"


@dataclass
class RelationshipMetadata:
    """Metadata about relationship between tables."""
    target_table: str
    foreign_key: str | Tuple[str, ...]  # FK column(s) in source table
    target_primary_key: str | Tuple[str, ...] = "id"  # Target PK column(s)
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
class SchemaMetadata:
    """Schema metadata for SQL compilation."""
    tables: Dict[str, Table]  # table_name -> SQLAlchemy Table object
    relationships: Dict[str, Dict[str, RelationshipMetadata]]  # table -> {field -> relationship}
    root_table: str  # Root table name (starting point)

    def get_table(self, table_name: str) -> Table:
        """Get SQLAlchemy Table by name."""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema")
        return self.tables[table_name]


class CompilationContext:
    """Context for tracking compilation state."""

    def __init__(self, schema: SchemaMetadata):
        self.schema = schema
        self.current_table = schema.root_table
        self.joined_tables: Set[str] = {schema.root_table}
        self.join_conditions: List[BinaryExpression] = []
        self.where_conditions: List[BinaryExpression] = []
        self.select_columns: List[Column] = []

    def get_table(self, table_name: str) -> Table:
        """Get SQLAlchemy Table by name."""
        return self.schema.get_table(table_name)

    def get_current_table(self) -> Table:
        """Get current table being processed."""
        return self.get_table(self.current_table)

    def get_relationship(self, field_name: str) -> Optional[RelationshipMetadata]:
        """Get relationship metadata for field in current table."""
        if self.current_table not in self.schema.relationships:
            return None
        return self.schema.relationships[self.current_table].get(field_name)

    def add_join(self, target_table: str, relationship: RelationshipMetadata):
        """Add JOIN to target table. Supports composite keys."""
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

        # Create join conditions
        # For ONE_TO_MANY: FK is in target table, PK is in source table
        # For MANY_TO_ONE: FK is in source table, PK is in target table
        if relationship.relationship_type == RelationType.ONE_TO_MANY:
            # FK columns in target table, PK columns in source table
            join_conditions = [
                target_table_obj.c[fk] == source_table.c[pk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # MANY_TO_ONE or MANY_TO_MANY
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

        self.join_conditions.append((target_table_obj, join_condition))
        self.joined_tables.add(target_table)

    def add_where(self, condition: BinaryExpression):
        """Add WHERE condition."""
        self.where_conditions.append(condition)


class SelectorCompiler:
    """Compiler for JSONPath selectors."""

    def __init__(self, context: CompilationContext):
        self.context = context

    def compile_selector(self, selector):
        """Compile a selector to SQLAlchemy."""
        if isinstance(selector, NameSelector):
            return self._compile_name_selector(selector)
        elif isinstance(selector, WildcardSelector):
            return self._compile_wildcard_selector(selector)
        elif isinstance(selector, FilterSelector):
            return self._compile_filter_selector(selector)
        else:
            raise NotImplementedError(f"Selector type {type(selector).__name__} not supported")

    def _compile_name_selector(self, selector: NameSelector):
        """Compile name selector (field access)."""
        field_name = selector.name

        # Check if this is a relationship
        relationship = self.context.get_relationship(field_name)

        if relationship:
            # This is a relationship - add JOIN and navigate
            self.context.add_join(relationship.target_table, relationship)
            self.context.current_table = relationship.target_table
        else:
            # This is a column - add to SELECT
            current_table = self.context.get_current_table()
            if field_name in current_table.c:
                self.context.select_columns.append(current_table.c[field_name])
            else:
                raise ValueError(
                    f"Column '{field_name}' not found in table '{self.context.current_table}'"
                )

    def _compile_wildcard_selector(self, selector: WildcardSelector):
        """Compile wildcard selector ([*])."""
        # Select all columns from current table
        current_table = self.context.get_current_table()
        for column in current_table.c:
            if column not in self.context.select_columns:
                self.context.select_columns.append(column)

    def _compile_filter_selector(self, selector: FilterSelector):
        """Compile filter selector ([?condition])."""
        # Compile filter expression to WHERE clause
        filter_expr = selector.expression.expression
        condition = self._compile_filter_expression(filter_expr)
        self.context.add_where(condition)

    def _compile_filter_expression(self, expr) -> BinaryExpression:
        """Compile filter expression to SQLAlchemy condition."""
        from jsonpath_rfc9535.filter_expressions import PrefixExpression, RelativeFilterQuery

        if isinstance(expr, ComparisonExpression):
            return self._compile_comparison(expr)
        elif isinstance(expr, LogicalExpression):
            return self._compile_logical(expr)
        elif isinstance(expr, PrefixExpression):
            return self._compile_prefix(expr)
        elif isinstance(expr, RelativeFilterQuery):
            # Standalone nested wildcard query (used as boolean expression)
            # e.g., @.items[*][?@.price > 100] without comparison
            return self._compile_nested_wildcard_operand(expr)
        else:
            raise NotImplementedError(f"Expression type {type(expr).__name__} not supported")

    def _compile_comparison(self, expr: ComparisonExpression) -> BinaryExpression:
        """Compile comparison expression."""
        # Get left operand (field reference)
        left_col = self._compile_filter_operand(expr.left)

        # Get right operand (literal)
        right_val = self._get_literal_value(expr.right)

        # Map operator to SQLAlchemy comparison
        if expr.operator == '==':
            return left_col == right_val
        elif expr.operator == '!=':
            return left_col != right_val
        elif expr.operator == '>':
            return left_col > right_val
        elif expr.operator == '<':
            return left_col < right_val
        elif expr.operator == '>=':
            return left_col >= right_val
        elif expr.operator == '<=':
            return left_col <= right_val
        else:
            raise NotImplementedError(f"Operator '{expr.operator}' not supported")

    def _compile_logical(self, expr: LogicalExpression) -> BinaryExpression:
        """Compile logical expression (AND, OR)."""
        left = self._compile_filter_expression(expr.left)
        right = self._compile_filter_expression(expr.right)

        # Determine operator
        expr_str = str(expr)
        if '&&' in expr_str or ' and ' in expr_str.lower():
            return and_(left, right)
        elif '||' in expr_str or ' or ' in expr_str.lower():
            return or_(left, right)
        else:
            # Fallback: assume AND
            return and_(left, right)

    def _compile_prefix(self, expr) -> BinaryExpression:
        """Compile prefix expression (NOT)."""
        if expr.operator == '!':
            inner = self._compile_filter_expression(expr.right)
            return not_(inner)
        else:
            raise NotImplementedError(f"Prefix operator '{expr.operator}' not supported")

    def _compile_filter_operand(self, operand) -> Column:
        """Compile filter operand to SQLAlchemy Column."""
        from jsonpath_rfc9535.filter_expressions import RelativeFilterQuery

        if isinstance(operand, RelativeFilterQuery):
            # @.field reference, nested path @.orders.total, or nested wildcard @.items[*][?...]
            query = operand.query

            if query.segments and len(query.segments) > 0:
                # Check if this is a nested wildcard pattern
                if self._is_nested_wildcard_query(query):
                    # This should not be called for nested wildcards in column context
                    # but just in case, delegate to nested wildcard handler
                    raise NotImplementedError(
                        "Nested wildcards should be handled as expressions, not column operands"
                    )

                # Check if this is a nested path (e.g., @.orders.total)
                if self._is_nested_path_query(query):
                    return self._compile_nested_path(query)

                # Simple field reference: @.field
                segment = query.segments[0]
                if segment.selectors and len(segment.selectors) > 0:
                    selector = segment.selectors[0]
                    if isinstance(selector, NameSelector):
                        field_name = selector.name
                        current_table = self.context.get_current_table()
                        if field_name in current_table.c:
                            return current_table.c[field_name]
                        else:
                            raise ValueError(
                                f"Column '{field_name}' not found in table '{self.context.current_table}'"
                            )
            raise NotImplementedError(f"Complex filter query not supported: {operand}")
        else:
            raise NotImplementedError(f"Operand type {type(operand).__name__} not supported in column context")

    def _compile_nested_wildcard_operand(self, operand) -> BinaryExpression:
        """
        Compile nested wildcard operand to EXISTS expression.

        This is called when RelativeFilterQuery appears as a standalone expression.
        """
        from jsonpath_rfc9535.filter_expressions import RelativeFilterQuery

        if isinstance(operand, RelativeFilterQuery):
            query = operand.query
            if self._is_nested_wildcard_query(query):
                return self._compile_nested_wildcard_exists(query)

        raise NotImplementedError(f"Operand type {type(operand).__name__} not supported as expression")

    def _is_nested_wildcard_query(self, query) -> bool:
        """
        Check if query represents a nested wildcard pattern like @.items[*][?...].

        Pattern structure:
          segments[0]: NameSelector (field name like "items")
          segments[1]: WildcardSelector ([*])
          segments[2]: FilterSelector ([?...])
        """
        if len(query.segments) < 3:
            return False

        # Check first segment is NameSelector
        if not (query.segments[0].selectors and
                isinstance(query.segments[0].selectors[0], NameSelector)):
            return False

        # Check second segment is WildcardSelector
        if not (query.segments[1].selectors and
                isinstance(query.segments[1].selectors[0], WildcardSelector)):
            return False

        # Check third segment is FilterSelector
        if not (query.segments[2].selectors and
                isinstance(query.segments[2].selectors[0], FilterSelector)):
            return False

        return True

    def _is_nested_path_query(self, query) -> bool:
        """
        Check if query represents a nested path like @.orders.total.

        Pattern structure:
          - Multiple segments, each with a single NameSelector
          - At least 2 segments
          - First N-1 segments are relationships
          - Last segment is a field in the final table

        Examples:
          @.orders.total -> True (2 segments)
          @.user.address.city -> True (3 segments)
          @.name -> False (1 segment - simple field)
          @.items[*][?...] -> False (has WildcardSelector)
        """
        if len(query.segments) < 2:
            return False

        # All segments must have exactly one NameSelector
        for segment in query.segments:
            if not segment.selectors or len(segment.selectors) != 1:
                return False
            if not isinstance(segment.selectors[0], NameSelector):
                return False

        return True

    def _compile_nested_path(self, query) -> Column:
        """
        Compile nested path to SQLAlchemy Column with JOINs.

        Pattern: @.orders.total
        Compiles to: orders.c.total (with JOIN added to context)

        Args:
            query: JSONPath query with nested path pattern

        Returns:
            SQLAlchemy Column reference
        """
        # Save current table to restore later
        saved_table = self.context.current_table

        # Navigate through relationships (all segments except the last)
        for segment in query.segments[:-1]:
            field_name = segment.selectors[0].name
            relationship = self.context.get_relationship(field_name)

            if not relationship:
                raise ValueError(
                    f"Field '{field_name}' is not a relationship in table '{self.context.current_table}'"
                )

            # Add JOIN for this relationship
            self.context.add_join(relationship.target_table, relationship)
            self.context.current_table = relationship.target_table

        # Get the final field name from the last segment
        final_field = query.segments[-1].selectors[0].name
        target_table = self.context.get_current_table()

        # Verify the field exists in the target table
        if final_field not in target_table.c:
            raise ValueError(
                f"Column '{final_field}' not found in table '{self.context.current_table}'"
            )

        # Get the column reference
        column = target_table.c[final_field]

        # Restore original table context
        self.context.current_table = saved_table

        return column

    def _compile_nested_wildcard_exists(self, query) -> BinaryExpression:
        """
        Compile nested wildcard pattern to EXISTS subquery.

        Pattern: @.items[*][?@.price > 100]
        Compiles to:
          EXISTS (
            SELECT 1 FROM items
            WHERE items.category_id = categories.id
              AND items.price > 100
          )

        Args:
            query: JSONPath query with nested wildcard pattern

        Returns:
            SQLAlchemy EXISTS expression
        """
        # Get field name from first segment (e.g., "items")
        field_name = query.segments[0].selectors[0].name

        # Get relationship metadata
        relationship = self.context.get_relationship(field_name)
        if not relationship:
            raise ValueError(
                f"Field '{field_name}' is not a relationship in table '{self.context.current_table}'"
            )

        target_table = self.context.get_table(relationship.target_table)
        source_table = self.context.get_current_table()

        # Get filter expression from third segment
        filter_selector = query.segments[2].selectors[0]
        filter_expr = filter_selector.expression.expression

        # Create temporary context for nested query
        # Save current state
        saved_table = self.context.current_table

        # Switch to target table context
        self.context.current_table = relationship.target_table

        # Compile filter expression in target table context
        filter_condition = self._compile_filter_expression(filter_expr)

        # Restore context
        self.context.current_table = saved_table

        # Build JOIN condition based on relationship type
        fk_columns = relationship.get_foreign_key_columns()
        pk_columns = relationship.get_target_primary_key_columns()

        if relationship.relationship_type == RelationType.ONE_TO_MANY:
            # FK is in target table, PK is in source table
            join_conditions = [
                target_table.c[fk] == source_table.c[pk]
                for fk, pk in zip(fk_columns, pk_columns)
            ]
        else:  # MANY_TO_ONE
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

        # Combine join and filter conditions
        combined_condition = and_(join_condition, filter_condition)

        # Build EXISTS subquery
        subquery = select(1).select_from(target_table).where(combined_condition)

        return exists(subquery)

    def _get_literal_value(self, operand):
        """Get literal value from operand."""
        from jsonpath_rfc9535.filter_expressions import (
            IntegerLiteral,
            StringLiteral,
            BooleanLiteral,
            NullLiteral,
        )

        if isinstance(operand, IntegerLiteral):
            return operand.value
        elif isinstance(operand, StringLiteral):
            return operand.value
        elif isinstance(operand, BooleanLiteral):
            return operand.value
        elif isinstance(operand, NullLiteral):
            return None
        else:
            raise NotImplementedError(f"Literal type {type(operand).__name__} not supported")


class JSONPathToSQLAlchemyCompiler:
    """
    Compiler that converts JSONPath expressions to SQLAlchemy Select queries.
    """

    def __init__(self, schema: SchemaMetadata):
        """
        Initialize compiler with schema.

        Args:
            schema: Database schema metadata

        Raises:
            ImportError: If SQLAlchemy is not installed
        """
        if not HAS_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required for SQL compilation. "
                "Install it with: pip install sqlalchemy"
            )
        self.schema = schema
        self.env = JSONPathEnvironment()

    def compile(self, jsonpath_expr: str, params: dict = None) -> Select:
        """
        Compile JSONPath expression to SQLAlchemy Select query.

        Args:
            jsonpath_expr: JSONPath expression
            params: Parameter values for placeholders (not used in SQLAlchemy)

        Returns:
            SQLAlchemy Select query object

        Example:
            >>> schema = SchemaMetadata(...)
            >>> compiler = JSONPathToSQLAlchemyCompiler(schema)
            >>> query = compiler.compile("$.users[?@.age > 18]")
            >>> print(query)
        """
        # Parse JSONPath
        query = self.env.compile(jsonpath_expr)

        # Create compilation context
        context = CompilationContext(self.schema)

        # Compile segments
        selector_compiler = SelectorCompiler(context)

        for segment in query.segments:
            for selector in segment.selectors:
                selector_compiler.compile_selector(selector)

        # Build SQLAlchemy query
        return self._build_query(context)

    def _build_query(self, context: CompilationContext) -> Select:
        """Build SQLAlchemy Select query from compilation context."""
        root_table = context.get_table(context.schema.root_table)

        # SELECT clause
        if context.select_columns:
            query = select(*context.select_columns)
        else:
            # Select all from root table
            query = select(root_table)

        # FROM clause - always start from root table
        query = query.select_from(root_table)

        # JOINs
        for joined_table, join_condition in context.join_conditions:
            query = query.join(joined_table, join_condition)

        # WHERE clause
        if context.where_conditions:
            query = query.where(and_(*context.where_conditions))

        return query

    def compile_filter(self, filter_expr: str) -> BinaryExpression:
        """
        Compile JSONPath filter expression to SQLAlchemy WHERE clause.

        Args:
            filter_expr: JSONPath filter expression (e.g., "@.age > 18")

        Returns:
            SQLAlchemy BinaryExpression

        Example:
            >>> compiler = JSONPathToSQLAlchemyCompiler(schema)
            >>> where = compiler.compile_filter("@.age > 18 && @.active == true")
        """
        # Wrap in filter projection to parse
        full_expr = f"$[?{filter_expr}]"
        query = self.compile(full_expr)

        # Extract WHERE clause (it's embedded in the query)
        # This is a simplified approach
        return query.whereclause if hasattr(query, 'whereclause') else None


def compile_to_sqlalchemy(jsonpath_expr: str, schema: SchemaMetadata, params: dict = None) -> Select:
    """
    Compile JSONPath expression to SQLAlchemy Select query.

    Args:
        jsonpath_expr: JSONPath expression (e.g., "$.users[?@.age > 25]")
        schema: Database schema metadata
        params: Parameter values for placeholders

    Returns:
        SQLAlchemy Select query object

    Example:
        >>> schema = SchemaMetadata(...)
        >>> query = compile_to_sqlalchemy("$.users[?@.age > 25]", schema)
        >>> print(query)
    """
    compiler = JSONPathToSQLAlchemyCompiler(schema)
    return compiler.compile(jsonpath_expr, params)
