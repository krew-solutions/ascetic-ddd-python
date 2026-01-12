"""
JSONPath to SQL compiler using jsonpath-rfc9535 library (RFC 9535 compliant).

Compiles JSONPath expressions into SQL queries for normalized relational databases.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum
from jsonpath_rfc9535 import JSONPathEnvironment
from jsonpath_rfc9535.selectors import NameSelector, WildcardSelector, FilterSelector
from jsonpath_rfc9535.filter_expressions import ComparisonExpression, LogicalExpression


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
        self.joined_tables: Set[str] = {schema.root_table}
        self.join_conditions: List[str] = []
        self.where_conditions: List[str] = []
        self.select_columns: List[str] = []

    def get_table(self, table_name: str) -> TableDef:
        """Get table by name."""
        return self.schema.get_table(table_name)

    def get_current_table(self) -> TableDef:
        """Get current table being processed."""
        return self.get_table(self.current_table)

    def get_relationship(self, field_name: str) -> Optional[RelationshipDef]:
        """Get relationship metadata for field in current table."""
        if self.current_table not in self.schema.relationships:
            return None
        return self.schema.relationships[self.current_table].get(field_name)

    def add_join(self, target_table: str, relationship: RelationshipDef):
        """Add JOIN to target table. Supports composite keys."""
        if target_table in self.joined_tables:
            return  # Already joined

        source_table = self.current_table

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
        self.join_conditions.append(f"JOIN {target_table} ON {join_condition}")
        self.joined_tables.add(target_table)

    def add_where(self, condition: str):
        """Add WHERE condition."""
        self.where_conditions.append(condition)


class SelectorCompiler:
    """Compiler for JSONPath selectors."""

    def __init__(self, context: CompilationContext):
        self.context = context

    def compile_selector(self, selector):
        """Compile a selector to SQL."""
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
            if current_table.has_column(field_name):
                self.context.select_columns.append(f"{self.context.current_table}.{field_name}")
            else:
                raise ValueError(
                    f"Column '{field_name}' not found in table '{self.context.current_table}'"
                )

    def _compile_wildcard_selector(self, selector: WildcardSelector):
        """Compile wildcard selector ([*])."""
        # Select all columns from current table
        current_table = self.context.get_current_table()
        for col_name in current_table.get_column_list():
            col_ref = f"{self.context.current_table}.{col_name}"
            if col_ref not in self.context.select_columns:
                self.context.select_columns.append(col_ref)

    def _compile_filter_selector(self, selector: FilterSelector):
        """Compile filter selector ([?condition])."""
        # Compile filter expression to WHERE clause
        filter_expr = selector.expression.expression
        condition = self._compile_filter_expression(filter_expr)
        self.context.add_where(condition)

    def _compile_filter_expression(self, expr) -> str:
        """Compile filter expression to SQL condition."""
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
            return self._compile_filter_operand(expr)
        else:
            raise NotImplementedError(f"Expression type {type(expr).__name__} not supported")

    def _compile_comparison(self, expr: ComparisonExpression) -> str:
        """Compile comparison expression."""
        # Get left operand (field reference)
        left_sql = self._compile_filter_operand(expr.left)

        # Get right operand (literal)
        right_sql = self._compile_filter_operand(expr.right)

        # Map operator
        op_map = {
            '==': '=',
            '!=': '!=',
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
        }

        operator = op_map.get(expr.operator, expr.operator)

        return f"{left_sql} {operator} {right_sql}"

    def _compile_logical(self, expr: LogicalExpression) -> str:
        """Compile logical expression (AND, OR)."""
        # AND or OR
        left = self._compile_filter_expression(expr.left)
        right = self._compile_filter_expression(expr.right)

        # Determine operator (check expression string or type)
        expr_str = str(expr)
        if '&&' in expr_str or ' and ' in expr_str.lower():
            op = 'AND'
        elif '||' in expr_str or ' or ' in expr_str.lower():
            op = 'OR'
        else:
            # Fallback: assume AND
            op = 'AND'

        return f"({left} {op} {right})"

    def _compile_prefix(self, expr) -> str:
        """Compile prefix expression (NOT)."""
        # PrefixExpression has operator and right
        if expr.operator == '!':
            inner = self._compile_filter_expression(expr.right)
            return f"NOT ({inner})"
        else:
            raise NotImplementedError(f"Prefix operator '{expr.operator}' not supported")

    def _compile_filter_operand(self, operand) -> str:
        """Compile filter operand (field reference or literal)."""
        from jsonpath_rfc9535.filter_expressions import (
            RelativeFilterQuery,
            IntegerLiteral,
            StringLiteral,
            BooleanLiteral,
            NullLiteral,
        )

        if isinstance(operand, RelativeFilterQuery):
            # @.field reference, nested path @.orders.total, or nested wildcard @.items[*][?...]
            query = operand.query

            if query.segments and len(query.segments) > 0:
                # Check if this is a nested wildcard pattern
                if self._is_nested_wildcard_query(query):
                    return self._compile_nested_wildcard_exists(query)

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
                        if current_table.has_column(field_name):
                            return f"{self.context.current_table}.{field_name}"
                        else:
                            raise ValueError(
                                f"Column '{field_name}' not found in table '{self.context.current_table}'"
                            )
            raise NotImplementedError(f"Complex filter query not supported: {operand}")

        elif isinstance(operand, IntegerLiteral):
            return str(operand.value)

        elif isinstance(operand, StringLiteral):
            # Escape single quotes
            escaped = operand.value.replace("'", "''")
            return f"'{escaped}'"

        elif isinstance(operand, BooleanLiteral):
            return 'TRUE' if operand.value else 'FALSE'

        elif isinstance(operand, NullLiteral):
            return 'NULL'

        else:
            raise NotImplementedError(f"Operand type {type(operand).__name__} not supported")

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

    def _compile_nested_path(self, query) -> str:
        """
        Compile nested path to SQL with JOINs.

        Pattern: @.orders.total
        Compiles to: orders.total (with JOIN added to context)

        Args:
            query: JSONPath query with nested path pattern

        Returns:
            Qualified column reference (e.g., "orders.total")
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
        target_table = self.context.current_table

        # Verify the field exists in the target table
        table_def = self.context.get_current_table()
        if not table_def.has_column(final_field):
            raise ValueError(
                f"Column '{final_field}' not found in table '{target_table}'"
            )

        # Restore original table context
        self.context.current_table = saved_table

        # Return qualified column reference
        return f"{target_table}.{final_field}"

    def _compile_nested_wildcard_exists(self, query) -> str:
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
            SQL EXISTS clause
        """
        # Get field name from first segment (e.g., "items")
        field_name = query.segments[0].selectors[0].name

        # Get relationship metadata
        relationship = self.context.get_relationship(field_name)
        if not relationship:
            raise ValueError(
                f"Field '{field_name}' is not a relationship in table '{self.context.current_table}'"
            )

        target_table = relationship.target_table
        source_table = self.context.current_table

        # Get filter expression from third segment
        filter_selector = query.segments[2].selectors[0]
        filter_expr = filter_selector.expression.expression

        # Create temporary context for nested query
        # Save current state
        saved_table = self.context.current_table

        # Switch to target table context
        self.context.current_table = target_table

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


class JSONPathToSQLCompiler:
    """
    Compiler that converts JSONPath expressions to SQL queries.
    """

    def __init__(self, schema: SchemaDef):
        """
        Initialize compiler with schema.

        Args:
            schema: Database schema definition
        """
        self.schema = schema
        self.env = JSONPathEnvironment()

    def compile(self, jsonpath_expr: str, params: dict = None) -> str:
        """
        Compile JSONPath expression to SQL.

        Args:
            jsonpath_expr: JSONPath expression
            params: Parameter values for placeholders (not used in raw SQL)

        Returns:
            SQL query string

        Example:
            >>> schema = SchemaDef(...)
            >>> compiler = JSONPathToSQLCompiler(schema)
            >>> sql = compiler.compile("$.users[?@.age > 18]")
            >>> print(sql)
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

        # Build SQL query
        return self._build_sql(context)

    def _build_sql(self, context: CompilationContext) -> str:
        """Build SQL query from compilation context."""
        # SELECT clause
        if context.select_columns:
            select_clause = f"SELECT {', '.join(context.select_columns)}"
        else:
            # Select all from root table
            select_clause = f"SELECT {context.schema.root_table}.*"

        # FROM clause
        from_clause = f"FROM {context.schema.root_table}"

        # JOINs
        join_clause = ""
        if context.join_conditions:
            join_clause = "\n" + "\n".join(context.join_conditions)

        # WHERE clause
        where_clause = ""
        if context.where_conditions:
            where_clause = "\nWHERE " + " AND ".join(context.where_conditions)

        # Combine
        sql = select_clause + "\n" + from_clause + join_clause + where_clause

        return sql

    def compile_filter(self, filter_expr: str) -> str:
        """
        Compile JSONPath filter expression to SQL WHERE clause.

        Args:
            filter_expr: JSONPath filter expression (e.g., "@.age > 18")

        Returns:
            SQL WHERE clause

        Example:
            >>> compiler = JSONPathToSQLCompiler(schema)
            >>> where = compiler.compile_filter("@.age > 18 && @.active == true")
        """
        # Wrap in filter projection to parse
        full_expr = f"$[?{filter_expr}]"
        sql = self.compile(full_expr)

        # Extract WHERE clause
        if "WHERE" in sql:
            return sql.split("WHERE")[1].strip()
        return ""


def compile_to_sql(jsonpath_expr: str, schema: SchemaDef, params: dict = None) -> str:
    """
    Compile JSONPath expression to SQL query.

    Args:
        jsonpath_expr: JSONPath expression (e.g., "$.users[?@.age > 25]")
        schema: Database schema definition
        params: Parameter values for placeholders

    Returns:
        SQL query string

    Example:
        >>> schema = SchemaDef(...)
        >>> sql = compile_to_sql("$.users[?@.age > 25]", schema)
        >>> print(sql)
    """
    compiler = JSONPathToSQLCompiler(schema)
    return compiler.compile(jsonpath_expr, params)
