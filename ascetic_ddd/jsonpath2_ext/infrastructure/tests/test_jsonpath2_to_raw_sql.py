"""
Unit tests for JSONPath to Raw SQL compiler.

Tests the compilation of JSONPath expressions to raw SQL queries.
"""
import unittest

from ..jsonpath2_to_raw_sql import (
    JSONPathToRawSQLCompiler, SchemaDef, TableDef, ColumnDef, RelationshipDef,
    RelationType, SQLQuery
)


class TestJSONPathToRawSQLCompiler(unittest.TestCase):
    """Test cases for JSONPath to Raw SQL compiler."""

    def setUp(self):
        """Set up test schema before each test."""
        # Define tables
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "email": ColumnDef("email", "VARCHAR(255)", nullable=True),
            },
            primary_key="id",
        )

        orders_table = TableDef(
            name="orders",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False),
                "total": ColumnDef("total", "DECIMAL(10,2)", nullable=False),
                "status": ColumnDef("status", "VARCHAR(50)", nullable=False),
            },
            primary_key="id",
        )

        order_items_table = TableDef(
            name="order_items",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "order_id": ColumnDef("order_id", "INTEGER", nullable=False),
                "product_id": ColumnDef("product_id", "INTEGER", nullable=False),
                "quantity": ColumnDef("quantity", "INTEGER", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
            },
            primary_key="id",
        )

        products_table = TableDef(
            name="products",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
                "category": ColumnDef("category", "VARCHAR(50)", nullable=True),
            },
            primary_key="id",
        )

        # Define relationships
        relationships = {
            "users": {
                "orders": RelationshipDef(
                    target_table="orders",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            "orders": {
                "items": RelationshipDef(
                    target_table="order_items",
                    foreign_key="order_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            "order_items": {
                "product": RelationshipDef(
                    target_table="products",
                    foreign_key="product_id",
                    target_primary_key="id",
                    relationship_type=RelationType.MANY_TO_ONE,
                ),
            },
        }

        # Create schema
        self.schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
                "order_items": order_items_table,
                "products": products_table,
            },
            relationships=relationships,
            root_table="users",
        )

        # Create compiler
        self.compiler = JSONPathToRawSQLCompiler(self.schema)

    def test_simple_field_access(self):
        """Test simple field access: $.name"""
        sql = self.compiler.compile("$.name")
        self.assertIn("SELECT users.name", sql)
        self.assertIn("FROM users", sql)
        self.assertNotIn("JOIN", sql)
        self.assertNotIn("WHERE", sql)

    def test_multiple_field_access(self):
        """Test multiple field access: $.name and $.email"""
        sql = self.compiler.compile("$.name")
        self.assertIn("users.name", sql)

    def test_wildcard(self):
        """Test wildcard: $[*]"""
        sql = self.compiler.compile("$[*]")
        self.assertIn("SELECT users.*", sql)
        self.assertIn("FROM users", sql)

    def test_filter_greater_than(self):
        """Test filter with > operator: $[?(@.age > 18)]"""
        sql = self.compiler.compile("$[?(@.age > 18)]")
        self.assertIn("FROM users", sql)
        self.assertIn("WHERE users.age > 18", sql)

    def test_filter_less_than(self):
        """Test filter with < operator: $[?(@.age < 65)]"""
        sql = self.compiler.compile("$[?(@.age < 65)]")
        self.assertIn("WHERE users.age < 65", sql)

    def test_filter_equal(self):
        """Test filter with = operator: $[?(@.name = \"John\")]"""
        sql = self.compiler.compile('$[?(@.name = "John")]')
        self.assertIn("WHERE users.name = 'John'", sql)

    def test_filter_not_equal(self):
        """Test filter with != operator: $[?(@.status != \"deleted\")]"""
        # Change root table to orders for this test
        schema = SchemaDef(
            tables=self.schema.tables,
            relationships=self.schema.relationships,
            root_table="orders",
        )
        compiler = JSONPathToRawSQLCompiler(schema)
        sql = compiler.compile('$[?(@.status != "deleted")]')
        self.assertIn("WHERE orders.status != 'deleted'", sql)

    def test_filter_greater_or_equal(self):
        """Test filter with >= operator: $[?(@.age >= 21)]"""
        sql = self.compiler.compile("$[?(@.age >= 21)]")
        self.assertIn("WHERE users.age >= 21", sql)

    def test_filter_less_or_equal(self):
        """Test filter with <= operator: $[?(@.age <= 100)]"""
        sql = self.compiler.compile("$[?(@.age <= 100)]")
        self.assertIn("WHERE users.age <= 100", sql)

    def test_navigation_to_related_table(self):
        """Test navigation to related table: $.orders"""
        sql = self.compiler.compile("$.orders")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)

    def test_navigation_with_wildcard(self):
        """Test navigation with wildcard: $.orders[*]"""
        sql = self.compiler.compile("$.orders[*]")
        self.assertIn("SELECT orders.*", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)

    def test_navigation_with_filter(self):
        """Test navigation with filter: $.orders[?(@.total > 100)]"""
        sql = self.compiler.compile("$.orders[?(@.total > 100)]")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("WHERE orders.total > 100", sql)

    def test_deep_navigation(self):
        """Test deep navigation: $.orders.items"""
        sql = self.compiler.compile("$.orders.items")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("JOIN order_items ON orders.id = order_items.order_id", sql)

    def test_deep_navigation_with_filter(self):
        """Test deep navigation with filter: $.orders.items[?(@.quantity > 5)]"""
        sql = self.compiler.compile("$.orders.items[?(@.quantity > 5)]")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("JOIN order_items", sql)
        self.assertIn("WHERE order_items.quantity > 5", sql)

    def test_very_deep_navigation(self):
        """Test very deep navigation: $.orders.items.product"""
        sql = self.compiler.compile("$.orders.items.product")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("JOIN order_items", sql)
        self.assertIn("JOIN products ON order_items.product_id = products.id", sql)

    def test_deep_navigation_with_final_filter(self):
        """Test deep navigation with final filter: $.orders.items.product[?(@.price < 50)]"""
        sql = self.compiler.compile("$.orders.items.product[?(@.price < 50)]")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("JOIN order_items", sql)
        self.assertIn("JOIN products", sql)
        self.assertIn("WHERE products.price < 50", sql)

    def test_string_value_escaping(self):
        """Test string value escaping: $[?(@.name = 'O''Brien')]"""
        sql = self.compiler.compile("$[?(@.name = \"O'Brien\")]")
        # Single quotes should be escaped as double single quotes
        self.assertIn("'O''Brien'", sql)

    def test_numeric_value_types(self):
        """Test numeric value types in filters."""
        # Integer
        sql = self.compiler.compile("$[?(@.age > 18)]")
        self.assertIn("18", sql)
        self.assertNotIn("'18'", sql)

        # Float
        schema = SchemaDef(
            tables=self.schema.tables,
            relationships=self.schema.relationships,
            root_table="orders",
        )
        compiler = JSONPathToRawSQLCompiler(schema)
        sql = compiler.compile("$[?(@.total > 99.99)]")
        self.assertIn("99.99", sql)
        self.assertNotIn("'99.99'", sql)

    def test_invalid_table_name(self):
        """Test error handling for invalid table name."""
        with self.assertRaises(ValueError) as ctx:
            bad_schema = SchemaDef(
                tables=self.schema.tables,
                relationships=self.schema.relationships,
                root_table="nonexistent",
            )
            compiler = JSONPathToRawSQLCompiler(bad_schema)
            compiler.compile("$.field")

        self.assertIn("not found", str(ctx.exception))

    def test_invalid_column_name(self):
        """Test error handling for invalid column name."""
        with self.assertRaises(ValueError) as ctx:
            self.compiler.compile("$.nonexistent_column")

        self.assertIn("not found", str(ctx.exception))

    def test_invalid_relationship(self):
        """Test error handling for invalid relationship."""
        with self.assertRaises(ValueError) as ctx:
            self.compiler.compile("$.nonexistent_relation")

        self.assertIn("not found", str(ctx.exception))

    def test_no_duplicate_joins(self):
        """Test that duplicate joins are not created."""
        # This would create duplicate join if not handled properly
        sql = self.compiler.compile("$.orders[?(@.total > 100)]")

        # Count occurrences of JOIN
        join_count = sql.count("JOIN orders")
        self.assertEqual(join_count, 1, "Should only have one JOIN for orders table")

    def test_field_selection_on_root_table(self):
        """Test field selection on root table."""
        sql = self.compiler.compile("$.email")
        self.assertIn("SELECT users.email", sql)
        self.assertIn("FROM users", sql)

    def test_combined_navigation_and_selection(self):
        """Test combined navigation and field selection."""
        sql = self.compiler.compile("$.orders.status")
        self.assertIn("SELECT orders.status", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)


class TestSQLQuery(unittest.TestCase):
    """Test cases for SQLQuery class."""

    def test_simple_select(self):
        """Test simple SELECT query building."""

        query = SQLQuery("users")
        query.add_select_column("users", "name")
        query.add_select_column("users", "email")

        sql = query.build()
        self.assertIn("SELECT users.name, users.email", sql)
        self.assertIn("FROM users", sql)

    def test_select_all(self):
        """Test SELECT * query building."""

        query = SQLQuery("users")
        query.add_select_all("users")

        sql = query.build()
        self.assertIn("SELECT users.*", sql)

    def test_join(self):
        """Test JOIN clause building."""

        query = SQLQuery("users")
        query.add_join("orders", "users", "user_id", "id")

        sql = query.build()
        # Low-level API assumes FK is in source table: source.fk = target.pk
        self.assertIn("JOIN orders ON users.user_id = orders.id", sql)

    def test_where(self):
        """Test WHERE clause building."""

        query = SQLQuery("users")
        query.add_where("users.age > 18")
        query.add_where("users.status = 'active'")

        sql = query.build()
        self.assertIn("WHERE users.age > 18 AND users.status = 'active'", sql)

    def test_full_query(self):
        """Test full query with all clauses."""

        query = SQLQuery("users")
        query.add_select_column("users", "name")
        query.add_select_column("orders", "total")
        query.add_join("orders", "users", "user_id", "id")
        query.add_where("orders.total > 100")

        sql = query.build()
        self.assertIn("SELECT users.name, orders.total", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("WHERE orders.total > 100", sql)


class TestTableDef(unittest.TestCase):
    """Test cases for TableDef class."""

    def test_has_column(self):
        """Test has_column method."""
        table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)"),
            },
        )

        self.assertTrue(table.has_column("id"))
        self.assertTrue(table.has_column("name"))
        self.assertFalse(table.has_column("nonexistent"))

    def test_get_column_list(self):
        """Test get_column_list method."""
        table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)"),
                "email": ColumnDef("email", "VARCHAR(255)"),
            },
        )

        columns = table.get_column_list()
        self.assertEqual(len(columns), 3)
        self.assertIn("id", columns)
        self.assertIn("name", columns)
        self.assertIn("email", columns)

    def test_composite_primary_key(self):
        """Test composite primary key support."""
        table = TableDef(
            name="user_roles",
            columns={
                "user_id": ColumnDef("user_id", "INTEGER", primary_key=True),
                "role_id": ColumnDef("role_id", "INTEGER", primary_key=True),
                "assigned_at": ColumnDef("assigned_at", "TIMESTAMP"),
            },
            primary_key=("user_id", "role_id"),
        )

        pk_columns = table.get_primary_key_columns()
        self.assertEqual(len(pk_columns), 2)
        self.assertIn("user_id", pk_columns)
        self.assertIn("role_id", pk_columns)


class TestCompositeKeys(unittest.TestCase):
    """Test cases for composite primary and foreign keys."""

    def setUp(self):
        """Set up test schema with composite keys."""
        # Table with composite PK: user_roles (user_id, role_id)
        user_roles_table = TableDef(
            name="user_roles",
            columns={
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False, primary_key=True),
                "role_id": ColumnDef("role_id", "INTEGER", nullable=False, primary_key=True),
                "assigned_at": ColumnDef("assigned_at", "TIMESTAMP", nullable=False),
                "assigned_by": ColumnDef("assigned_by", "INTEGER", nullable=True),
            },
            primary_key=("user_id", "role_id"),  # Composite PK
        )

        # Table with composite FK referencing user_roles
        role_permissions_table = TableDef(
            name="role_permissions",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False),
                "role_id": ColumnDef("role_id", "INTEGER", nullable=False),
                "permission": ColumnDef("permission", "VARCHAR(100)", nullable=False),
                "granted_at": ColumnDef("granted_at", "TIMESTAMP", nullable=False),
            },
            primary_key="id",
        )

        # Users table (referenced by user_roles)
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "email": ColumnDef("email", "VARCHAR(255)", nullable=False),
            },
            primary_key="id",
        )

        # Roles table (referenced by user_roles)
        roles_table = TableDef(
            name="roles",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(50)", nullable=False),
                "description": ColumnDef("description", "TEXT", nullable=True),
            },
            primary_key="id",
        )

        # Define relationships
        relationships = {
            "user_roles": {
                "permissions": RelationshipDef(
                    target_table="role_permissions",
                    foreign_key=("user_id", "role_id"),  # Composite FK
                    target_primary_key=("user_id", "role_id"),  # Composite target PK
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
                "user": RelationshipDef(
                    target_table="users",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type=RelationType.MANY_TO_ONE,
                ),
                "role": RelationshipDef(
                    target_table="roles",
                    foreign_key="role_id",
                    target_primary_key="id",
                    relationship_type=RelationType.MANY_TO_ONE,
                ),
            },
        }

        # Create schema
        self.schema = SchemaDef(
            tables={
                "user_roles": user_roles_table,
                "role_permissions": role_permissions_table,
                "users": users_table,
                "roles": roles_table,
            },
            relationships=relationships,
            root_table="user_roles",
        )

        # Create compiler
        self.compiler = JSONPathToRawSQLCompiler(self.schema)

    def test_composite_pk_table_access(self):
        """Test accessing table with composite primary key."""
        sql = self.compiler.compile("$[*]")
        self.assertIn("SELECT user_roles.*", sql)
        self.assertIn("FROM user_roles", sql)

    def test_composite_fk_join(self):
        """Test JOIN using composite foreign key."""
        sql = self.compiler.compile("$.permissions[*]")
        self.assertIn("SELECT role_permissions.*", sql)
        self.assertIn("FROM user_roles", sql)
        # Should generate: JOIN role_permissions ON user_roles.user_id = role_permissions.user_id AND user_roles.role_id = role_permissions.role_id
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)

    def test_composite_fk_with_filter(self):
        """Test composite FK JOIN with WHERE filter."""
        sql = self.compiler.compile('$.permissions[?(@.permission = "admin")]')
        self.assertIn("FROM user_roles", sql)
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)
        self.assertIn("WHERE role_permissions.permission = 'admin'", sql)

    def test_mixed_single_and_composite_keys(self):
        """Test navigation with both single and composite keys."""
        sql = self.compiler.compile("$.user[*]")
        self.assertIn("FROM user_roles", sql)
        self.assertIn("JOIN users ON user_roles.user_id = users.id", sql)

    def test_composite_pk_field_selection(self):
        """Test selecting fields from table with composite PK."""
        sql = self.compiler.compile("$.assigned_at")
        self.assertIn("SELECT user_roles.assigned_at", sql)
        self.assertIn("FROM user_roles", sql)

    def test_composite_fk_field_selection_after_join(self):
        """Test selecting fields after composite FK JOIN."""
        sql = self.compiler.compile("$.permissions.permission")
        self.assertIn("SELECT role_permissions.permission", sql)
        self.assertIn("FROM user_roles", sql)
        self.assertIn("JOIN role_permissions ON", sql)


class TestParenthesesNormalization(unittest.TestCase):
    """Test automatic parentheses addition for RFC 9535 syntax compatibility."""

    def setUp(self):
        """Set up test schema before each test."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=True),
            },
            primary_key="id",
        )

        schema = SchemaDef(
            tables={"users": users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_filter_without_parentheses(self):
        """Test filter expression without parentheses (RFC 9535 syntax)."""
        # RFC 9535 allows $[?@.age > 25] without parentheses
        # jsonpath2 requires $[?(@.age > 25)]
        # Our compiler should auto-add parentheses
        sql = self.compiler.compile("$[?@.age > 25]")
        self.assertIn("WHERE users.age > 25", sql)

    def test_filter_with_parentheses(self):
        """Test filter expression with parentheses."""
        sql = self.compiler.compile("$[?(@.age > 25)]")
        self.assertIn("WHERE users.age > 25", sql)

    def test_both_syntaxes_produce_same_result(self):
        """Test that both syntaxes produce the same SQL."""
        sql_without = self.compiler.compile("$[?@.age > 25]")
        sql_with = self.compiler.compile("$[?(@.age > 25)]")
        self.assertEqual(sql_without, sql_with)

    def test_filter_without_parentheses_string_comparison(self):
        """Test string comparison without parentheses."""
        # jsonpath2 uses double quotes for strings
        sql = self.compiler.compile('$[?@.name == "Alice"]')
        self.assertIn("WHERE users.name = 'Alice'", sql)


class TestParenthesesSupport(unittest.TestCase):
    """Test parentheses support in filter expressions."""

    def setUp(self):
        """Set up test schema before each test."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=True),
            },
            primary_key="id",
        )

        schema = SchemaDef(
            tables={"users": users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_simple_parentheses(self):
        """Test simple parentheses around filter condition."""
        sql = self.compiler.compile("$[?(@.age > 25)]")
        self.assertIn("WHERE users.age > 25", sql)

    def test_nested_parentheses_single_condition(self):
        """Test nested parentheses with single condition."""
        sql = self.compiler.compile("$[?((@.age > 25))]")
        self.assertIn("WHERE users.age > 25", sql)

    def test_parentheses_with_comparison_operators(self):
        """Test parentheses with different comparison operators."""
        # Greater than
        sql = self.compiler.compile("$[?(@.age > 25)]")
        self.assertIn("users.age > 25", sql)

        # Less than
        sql = self.compiler.compile("$[?(@.age < 65)]")
        self.assertIn("users.age < 65", sql)

        # Equals
        sql = self.compiler.compile("$[?(@.age = 30)]")
        self.assertIn("users.age = 30", sql)


class TestNestedWildcardsSupport(unittest.TestCase):
    """Test nested wildcards (navigation to related tables) support."""

    def setUp(self):
        """Set up test schema with relationships."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
            },
            primary_key="id",
        )

        orders_table = TableDef(
            name="orders",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False),
                "total": ColumnDef("total", "DECIMAL(10,2)", nullable=False),
                "status": ColumnDef("status", "VARCHAR(50)", nullable=False),
            },
            primary_key="id",
        )

        order_items_table = TableDef(
            name="order_items",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "order_id": ColumnDef("order_id", "INTEGER", nullable=False),
                "product_name": ColumnDef("product_name", "VARCHAR(100)", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
            },
            primary_key="id",
        )

        relationships = {
            "users": {
                "orders": RelationshipDef(
                    target_table="orders",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            "orders": {
                "items": RelationshipDef(
                    target_table="order_items",
                    foreign_key="order_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
                "order_items": order_items_table,
            },
            relationships=relationships,
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_navigate_to_related_table(self):
        """Test navigation to related table via wildcard."""
        sql = self.compiler.compile("$.orders[*]")
        self.assertIn("SELECT orders.*", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON", sql)

    def test_navigate_and_filter(self):
        """Test navigation to related table with filter."""
        sql = self.compiler.compile("$.orders[*][?(@.total > 100)]")
        self.assertIn("SELECT orders.*", sql)
        self.assertIn("JOIN orders ON", sql)
        self.assertIn("WHERE orders.total > 100", sql)

    def test_navigate_two_levels(self):
        """Test navigation through two relationship levels."""
        sql = self.compiler.compile("$.orders[*].items[*]")
        self.assertIn("order_items.*", sql)
        self.assertIn("JOIN orders ON", sql)
        self.assertIn("JOIN order_items ON", sql)

    def test_navigate_two_levels_with_filter(self):
        """Test navigation through two levels with filter."""
        sql = self.compiler.compile("$.orders[*].items[*][?(@.price > 50)]")
        self.assertIn("order_items.*", sql)
        self.assertIn("JOIN orders ON", sql)
        self.assertIn("JOIN order_items ON", sql)
        self.assertIn("WHERE order_items.price > 50", sql)

    def test_select_field_from_related_table(self):
        """Test selecting specific field from related table."""
        sql = self.compiler.compile("$.orders[*].total")
        self.assertIn("orders.total", sql)
        self.assertIn("JOIN orders ON", sql)


class TestLogicalOperatorsSupport(unittest.TestCase):
    """Test logical operators (AND/OR) support."""

    def setUp(self):
        """Set up test schema."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=True),
                "role": ColumnDef("role", "VARCHAR(50)", nullable=True),
            },
            primary_key="id",
        )

        schema = SchemaDef(
            tables={"users": users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_and_operator(self):
        """Test AND logical operator."""
        sql = self.compiler.compile("$[?(@.age > 25 and @.active = true)]")
        self.assertIn("WHERE", sql)
        self.assertIn("users.age > 25", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.active = TRUE", sql)

    def test_or_operator(self):
        """Test OR logical operator."""
        sql = self.compiler.compile("$[?(@.age < 18 or @.age > 65)]")
        self.assertIn("WHERE", sql)
        self.assertIn("users.age < 18", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.age > 65", sql)

    def test_and_with_string_comparison(self):
        """Test AND with string comparison."""
        sql = self.compiler.compile('$[?(@.name = "Alice" and @.role = "admin")]')
        self.assertIn("WHERE", sql)
        self.assertIn("users.name = 'Alice'", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.role = 'admin'", sql)

    def test_or_with_different_fields(self):
        """Test OR with different fields."""
        sql = self.compiler.compile("$[?(@.active = true or @.role = \"admin\")]")
        self.assertIn("WHERE", sql)
        self.assertIn("users.active = TRUE", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.role = 'admin'", sql)

    def test_nested_logical_operators(self):
        """Test nested logical operators (AND within OR within AND)."""
        # Test: (age > 25 and active = true)
        sql = self.compiler.compile("$[?(@.age > 25 and @.active = true)]")
        self.assertIn("(users.age > 25 AND users.active = TRUE)", sql)

    def test_multiple_and_conditions(self):
        """Test multiple AND conditions."""
        sql = self.compiler.compile('$[?(@.age > 18 and @.active = true and @.role = "user")]')
        self.assertIn("users.age > 18", sql)
        self.assertIn("users.active = TRUE", sql)
        self.assertIn("users.role = 'user'", sql)
        # Should have two AND operators
        self.assertEqual(sql.count("AND"), 2)


class TestRFC9535LogicalOperatorsNormalization(unittest.TestCase):
    """Test RFC 9535 logical operators (&&, ||) normalization to jsonpath2 syntax."""

    def setUp(self):
        """Set up test schema."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=True),
            },
            primary_key="id",
        )

        schema = SchemaDef(
            tables={"users": users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_double_ampersand_to_and(self):
        """Test && is converted to AND."""
        sql = self.compiler.compile("$[?(@.age > 25 && @.active == true)]")
        self.assertIn("WHERE", sql)
        self.assertIn("users.age > 25", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.active = TRUE", sql)

    def test_double_pipe_to_or(self):
        """Test || is converted to OR."""
        sql = self.compiler.compile("$[?(@.age < 18 || @.age > 65)]")
        self.assertIn("WHERE", sql)
        self.assertIn("users.age < 18", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.age > 65", sql)

    def test_rfc9535_and_jsonpath2_produce_same_result(self):
        """Test that RFC 9535 and jsonpath2 syntaxes produce the same SQL."""
        # RFC 9535 syntax
        sql_rfc9535 = self.compiler.compile("$[?(@.age > 25 && @.active == true)]")
        # jsonpath2 syntax
        sql_jsonpath2 = self.compiler.compile("$[?(@.age > 25 and @.active = true)]")
        self.assertEqual(sql_rfc9535, sql_jsonpath2)

    def test_mixed_operators_not_in_string(self):
        """Test that && and || in string literals are not converted."""
        sql = self.compiler.compile('$[?(@.name == "A && B")]')
        self.assertIn("users.name = 'A && B'", sql)


class TestNestedPathsInFilters(unittest.TestCase):
    """Test nested paths in filter expressions (e.g., @.orders.total)."""

    def setUp(self):
        """Set up test schema with relationships."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
            },
            primary_key="id",
        )

        orders_table = TableDef(
            name="orders",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False),
                "total": ColumnDef("total", "DECIMAL(10,2)", nullable=False),
                "status": ColumnDef("status", "VARCHAR(50)", nullable=False),
            },
            primary_key="id",
        )

        order_items_table = TableDef(
            name="order_items",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "order_id": ColumnDef("order_id", "INTEGER", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
            },
            primary_key="id",
        )

        relationships = {
            "users": {
                "orders": RelationshipDef(
                    target_table="orders",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            "orders": {
                "items": RelationshipDef(
                    target_table="order_items",
                    foreign_key="order_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
                "order_items": order_items_table,
            },
            relationships=relationships,
            root_table="users",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_nested_path_one_level(self):
        """Test nested path @.orders.total with JOIN."""
        sql = self.compiler.compile("$[?(@.orders.total > 100)]")
        self.assertIn("SELECT users.*", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("WHERE orders.total > 100", sql)

    def test_nested_path_with_string_comparison(self):
        """Test nested path with string comparison."""
        sql = self.compiler.compile('$[?(@.orders.status = "completed")]')
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("WHERE orders.status = 'completed'", sql)

    def test_nested_path_two_levels(self):
        """Test nested path @.orders.items.price with two JOINs."""
        sql = self.compiler.compile("$[?(@.orders.items.price > 50)]")
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("JOIN order_items ON orders.id = order_items.order_id", sql)
        self.assertIn("WHERE order_items.price > 50", sql)

    def test_nested_path_with_and_operator(self):
        """Test nested path combined with AND operator."""
        sql = self.compiler.compile('$[?(@.orders.total > 100 and @.orders.status = "pending")]')
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)
        self.assertIn("orders.total > 100", sql)
        self.assertIn("AND", sql)
        self.assertIn("orders.status = 'pending'", sql)


class TestNestedWildcardExists(unittest.TestCase):
    """Test nested wildcard patterns compiled to EXISTS subqueries."""

    def setUp(self):
        """Set up test schema with relationships."""
        categories_table = TableDef(
            name="categories",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
            },
            primary_key="id",
        )

        items_table = TableDef(
            name="items",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "category_id": ColumnDef("category_id", "INTEGER", nullable=False),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
                "quantity": ColumnDef("quantity", "INTEGER", nullable=False),
            },
            primary_key="id",
        )

        relationships = {
            "categories": {
                "items": RelationshipDef(
                    target_table="items",
                    foreign_key="category_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        schema = SchemaDef(
            tables={"categories": categories_table, "items": items_table},
            relationships=relationships,
            root_table="categories",
        )

        self.compiler = JSONPathToRawSQLCompiler(schema)

    def test_nested_wildcard_simple_filter(self):
        """Test nested wildcard with simple filter generates EXISTS."""
        sql = self.compiler.compile("$[*][?(@.items[*][?(@.price > 100)])]")
        self.assertIn("SELECT categories.*", sql)
        self.assertIn("FROM categories", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("SELECT 1 FROM items", sql)
        self.assertIn("items.category_id = categories.id", sql)
        self.assertIn("items.price > 100", sql)

    def test_nested_wildcard_with_and_filter(self):
        """Test nested wildcard with AND in inner filter."""
        sql = self.compiler.compile(
            "$[*][?(@.items[*][?(@.price > 50 and @.price < 200)])]"
        )
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 50", sql)
        self.assertIn("AND", sql)
        self.assertIn("items.price < 200", sql)

    def test_nested_wildcard_string_comparison(self):
        """Test nested wildcard with string comparison."""
        sql = self.compiler.compile('$[*][?(@.items[*][?(@.name = "Laptop")])]')
        self.assertIn("EXISTS", sql)
        self.assertIn("items.name = 'Laptop'", sql)

    def test_nested_wildcard_combined_with_parent_filter(self):
        """Test nested wildcard combined with filter on parent."""
        sql = self.compiler.compile(
            '$[?(@.name = "Electronics")][?(@.items[*][?(@.price > 500)])]'
        )
        self.assertIn("categories.name = 'Electronics'", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 500", sql)

    def test_nested_wildcard_no_join_in_main_query(self):
        """Test that nested wildcard uses EXISTS, not JOIN."""
        sql = self.compiler.compile("$[*][?(@.items[*][?(@.price > 100)])]")
        # Should NOT have JOIN items in main query (only in EXISTS subquery)
        main_query_parts = sql.split("EXISTS")[0]
        self.assertNotIn("JOIN items", main_query_parts)


if __name__ == "__main__":
    unittest.main()
