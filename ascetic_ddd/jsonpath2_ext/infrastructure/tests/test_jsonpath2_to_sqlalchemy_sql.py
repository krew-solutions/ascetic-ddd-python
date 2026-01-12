"""
Unit tests for JSONPath to SQL compiler (ORM-based version with SQLAlchemy).

Tests JSONPath to SQLAlchemy compiler for normalized relational databases.
"""
import unittest

from sqlalchemy import Column, Integer, String, MetaData, Table, Boolean, Text, Numeric, ForeignKey

from ..jsonpath2_to_sqlalchemy_sql import (
    JSONPathToSQLCompiler, SchemaMetadata, RelationshipMetadata,
    CompilationContext
)


class TestRelationshipMetadata(unittest.TestCase):
    """Test RelationshipMetadata class."""

    def test_default_values(self):
        """Test default values for RelationshipMetadata."""
        rel = RelationshipMetadata(
            target_table="orders",
            foreign_key="user_id",
        )

        self.assertEqual(rel.target_table, "orders")
        self.assertEqual(rel.foreign_key, "user_id")
        self.assertEqual(rel.target_primary_key, "id")
        self.assertEqual(rel.relationship_type, "one-to-many")

    def test_custom_values(self):
        """Test custom values for RelationshipMetadata."""
        rel = RelationshipMetadata(
            target_table="products",
            foreign_key="product_id",
            target_primary_key="product_id",
            relationship_type="many-to-one",
        )

        self.assertEqual(rel.target_table, "products")
        self.assertEqual(rel.foreign_key, "product_id")
        self.assertEqual(rel.target_primary_key, "product_id")
        self.assertEqual(rel.relationship_type, "many-to-one")


class TestSchemaMetadata(unittest.TestCase):
    """Test SchemaMetadata class."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )

    def test_schema_creation(self):
        """Test SchemaMetadata creation."""
        schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.assertEqual(schema.root_table, "users")
        self.assertIn("users", schema.tables)
        self.assertEqual(schema.relationships, {})


class TestCompilationContext(unittest.TestCase):
    """Test CompilationContext class."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("age", Integer),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

    def test_initialization(self):
        """Test context initialization."""
        context = CompilationContext(self.schema)

        self.assertEqual(context.current_table, "users")
        self.assertIn("users", context.joined_tables)
        self.assertEqual(len(context.join_conditions), 0)
        self.assertEqual(len(context.where_conditions), 0)
        self.assertEqual(len(context.select_columns), 0)

    def test_get_table(self):
        """Test get_table method."""
        context = CompilationContext(self.schema)
        table = context.get_table("users")

        self.assertEqual(table.name, "users")

    def test_get_table_not_found(self):
        """Test get_table with non-existent table."""
        context = CompilationContext(self.schema)

        with self.assertRaises(ValueError) as cm:
            context.get_table("nonexistent")

        self.assertIn("not found", str(cm.exception))

    def test_get_current_table(self):
        """Test get_current_table method."""
        context = CompilationContext(self.schema)
        table = context.get_current_table()

        self.assertEqual(table.name, "users")


class TestJSONPathToSQLCompilerBasic(unittest.TestCase):
    """Test basic JSONPath to SQL compilation."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()

        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("age", Integer),
            Column("email", String),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_simple_field_access(self):
        """Test simple field access: $.name"""
        query = self.compiler.compile("$.name")

        # Verify query compiles
        sql = str(query)
        self.assertIn("users.name", sql)

    def test_wildcard(self):
        """Test wildcard: $[*]"""
        query = self.compiler.compile("$[*]")

        sql = str(query)
        self.assertIn("users", sql)

    def test_filter_simple(self):
        """Test simple filter: $[?(@.age > 18)]"""
        query = self.compiler.compile("$[?(@.age > 18)]")

        sql = str(query)
        self.assertIn("users.age >", sql)
        # SQLAlchemy uses bind parameters, not literals
        self.assertIn(":age_", sql)

    def test_filter_equality(self):
        """Test filter with equality: $[?(@.name = \"John\")]"""
        query = self.compiler.compile('$[?(@.name = "John")]')

        sql = str(query)
        self.assertIn("users.name", sql)
        # SQLAlchemy uses bind parameters
        self.assertIn(":name_", sql)

    def test_filter_not_equal(self):
        """Test filter with inequality: $[?(@.age != 25)]"""
        query = self.compiler.compile("$[?(@.age != 25)]")

        sql = str(query)
        self.assertIn("users.age", sql)
        # SQLAlchemy uses bind parameters
        self.assertIn(":age_", sql)

    def test_filter_greater_or_equal(self):
        """Test filter with >=: $[?(@.age >= 18)]"""
        query = self.compiler.compile("$[?(@.age >= 18)]")

        sql = str(query)
        self.assertIn("users.age >=", sql)

    def test_filter_less_than(self):
        """Test filter with <: $[?(@.age < 30)]"""
        query = self.compiler.compile("$[?(@.age < 30)]")

        sql = str(query)
        self.assertIn("users.age <", sql)

    def test_filter_less_or_equal(self):
        """Test filter with <=: $[?(@.age <= 65)]"""
        query = self.compiler.compile("$[?(@.age <= 65)]")

        sql = str(query)
        self.assertIn("users.age <=", sql)


# NOTE: More comprehensive tests with relationships are in TestCompositeKeys class


class TestJSONPathToSQLCompilerExamples(unittest.TestCase):
    """Test examples from jsonpath_to_sql_example.py"""

    def setUp(self):
        """Set up example schema."""
        self.metadata = MetaData()

        # Define tables
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("age", Integer),
            Column("email", String),
        )

        self.schema = SchemaMetadata(
            tables={
                "users": self.users_table,
            },
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_example_simple_field_access(self):
        """Example 1: Simple field access"""
        query = self.compiler.compile("$.name")
        sql = str(query)
        self.assertIn("users.name", sql)

    def test_example_wildcard(self):
        """Example 2: Wildcard (all columns)"""
        query = self.compiler.compile("$[*]")
        sql = str(query)
        self.assertIn("users", sql)

    def test_example_filter_on_age(self):
        """Example 3: Filter on age"""
        query = self.compiler.compile("$[?(@.age > 18)]")
        sql = str(query)
        self.assertIn("users.age >", sql)

    def test_example_filter_with_equality(self):
        """Example 4: Filter with equality"""
        query = self.compiler.compile('$[?(@.name = "John")]')
        sql = str(query)
        self.assertIn("users.name", sql)


class TestCompositeKeys(unittest.TestCase):
    """Test composite primary and foreign keys support."""

    def setUp(self):
        """Set up test schema with composite keys."""
        self.metadata = MetaData()

        # Table with composite PK: user_roles (user_id, role_id)
        self.user_roles_table = Table(
            "user_roles",
            self.metadata,
            Column("user_id", Integer, primary_key=True),
            Column("role_id", Integer, primary_key=True),
            Column("assigned_at", String),
            Column("assigned_by", Integer),
        )

        # Table with composite FK referencing user_roles
        self.role_permissions_table = Table(
            "role_permissions",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("role_id", Integer),
            Column("permission", String),
            Column("granted_at", String),
        )

        # Users table (referenced by user_roles)
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("email", String),
            Column("active", Boolean),
        )

        # Roles table (referenced by user_roles)
        self.roles_table = Table(
            "roles",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("description", Text),
            Column("level", Integer),
        )

        # Define relationships
        relationships = {
            "user_roles": {
                "permissions": RelationshipMetadata(
                    target_table="role_permissions",
                    foreign_key=("user_id", "role_id"),  # Composite FK
                    target_primary_key=("user_id", "role_id"),  # Composite PK in source
                    relationship_type="one-to-many",
                ),
                "user": RelationshipMetadata(
                    target_table="users",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type="many-to-one",
                ),
                "role": RelationshipMetadata(
                    target_table="roles",
                    foreign_key="role_id",
                    target_primary_key="id",
                    relationship_type="many-to-one",
                ),
            },
        }

        self.schema = SchemaMetadata(
            tables={
                "user_roles": self.user_roles_table,
                "role_permissions": self.role_permissions_table,
                "users": self.users_table,
                "roles": self.roles_table,
            },
            relationships=relationships,
            root_table="user_roles",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_composite_key_methods(self):
        """Test RelationshipMetadata methods for composite keys."""
        # Composite keys
        rel = RelationshipMetadata(
            target_table="target",
            foreign_key=("fk1", "fk2"),
            target_primary_key=("pk1", "pk2"),
        )

        self.assertEqual(rel.get_foreign_key_columns(), ["fk1", "fk2"])
        self.assertEqual(rel.get_target_primary_key_columns(), ["pk1", "pk2"])

        # Single keys
        rel_single = RelationshipMetadata(
            target_table="target",
            foreign_key="fk1",
            target_primary_key="pk1",
        )

        self.assertEqual(rel_single.get_foreign_key_columns(), ["fk1"])
        self.assertEqual(rel_single.get_target_primary_key_columns(), ["pk1"])

    def test_access_composite_pk_table(self):
        """Test access composite PK table with $[*]"""
        query = self.compiler.compile("$[*]")

        sql = str(query)
        self.assertIn("user_roles", sql)

    def test_select_field_from_composite_pk_table(self):
        """Test select field from composite PK table with $.assigned_at"""
        query = self.compiler.compile("$.assigned_at")

        sql = str(query)
        self.assertIn("user_roles.assigned_at", sql)

    def test_filter_on_composite_pk_table(self):
        """Test filter on composite PK table with $[?(@.assigned_by = 1)]"""
        query = self.compiler.compile("$[?(@.assigned_by = 1)]")

        sql = str(query)
        self.assertIn("user_roles.assigned_by", sql)
        self.assertIn("WHERE", sql.upper())

    def test_join_via_composite_fk(self):
        """Test JOIN via composite FK with $.permissions[*]"""
        query = self.compiler.compile("$.permissions[*]")

        sql = str(query)
        self.assertIn("role_permissions", sql)
        self.assertIn("JOIN", sql.upper())
        # Should have composite FK join with both conditions
        self.assertIn("user_id", sql)
        self.assertIn("role_id", sql)

    def test_join_via_composite_fk_with_filter(self):
        """Test JOIN via composite FK with filter $.permissions[?(@.permission = \"admin\")]"""
        query = self.compiler.compile('$.permissions[?(@.permission = "admin")]')

        sql = str(query)
        self.assertIn("role_permissions", sql)
        self.assertIn("JOIN", sql.upper())
        self.assertIn("permission", sql)

    def test_join_via_single_fk_to_users(self):
        """Test JOIN via single FK to users with $.user[*]"""
        query = self.compiler.compile("$.user[*]")

        sql = str(query)
        self.assertIn("users", sql)
        self.assertIn("JOIN", sql.upper())

    def test_join_via_single_fk_to_roles(self):
        """Test JOIN via single FK to roles with $.role[*]"""
        query = self.compiler.compile("$.role[*]")

        sql = str(query)
        self.assertIn("roles", sql)
        self.assertIn("JOIN", sql.upper())

    def test_select_after_composite_fk_join(self):
        """Test select after composite FK JOIN with $.permissions.permission"""
        query = self.compiler.compile("$.permissions.permission")

        sql = str(query)
        self.assertIn("role_permissions.permission", sql)
        self.assertIn("JOIN", sql.upper())

    def test_complex_filter_on_user(self):
        """Test complex filter after single FK JOIN $.user[?(@.active = true)]"""
        query = self.compiler.compile("$.user[?(@.active = true)]")

        sql = str(query)
        self.assertIn("users", sql)
        self.assertIn("active", sql)

    def test_complex_filter_on_role(self):
        """Test complex filter on role level $.role[?(@.level > 5)]"""
        query = self.compiler.compile("$.role[?(@.level > 5)]")

        sql = str(query)
        self.assertIn("roles", sql)
        self.assertIn("level", sql)

    def test_mismatched_composite_key_lengths(self):
        """Test error when FK and PK have different number of columns."""
        # Create invalid relationship
        bad_relationship = RelationshipMetadata(
            target_table="target",
            foreign_key=("fk1", "fk2"),
            target_primary_key=("pk1", "pk2", "pk3"),  # Different length!
        )

        context = CompilationContext(self.schema)

        with self.assertRaises(ValueError) as cm:
            context.add_join("role_permissions", bad_relationship)

        self.assertIn("same number of columns", str(cm.exception))


class TestErrorHandling(unittest.TestCase):
    """Test error handling."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_nonexistent_column(self):
        """Test accessing non-existent column."""
        with self.assertRaises(ValueError) as cm:
            self.compiler.compile("$.nonexistent")

        self.assertIn("not found", str(cm.exception))

    def test_invalid_jsonpath_syntax(self):
        """Test invalid JSONPath syntax."""
        with self.assertRaises(ValueError):
            self.compiler.compile("$[invalid")


class TestParenthesesNormalization(unittest.TestCase):
    """Test automatic parentheses addition for RFC 9535 syntax compatibility."""

    def setUp(self):
        """Set up test schema before each test."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("age", Integer),
            Column("active", Boolean),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_filter_without_parentheses(self):
        """Test filter expression without parentheses (RFC 9535 syntax)."""
        # RFC 9535 allows $[?@.age > 25] without parentheses
        # jsonpath2 requires $[?(@.age > 25)]
        # Our compiler should auto-add parentheses
        query = self.compiler.compile("$[?@.age > 25]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)

    def test_filter_with_parentheses(self):
        """Test filter expression with parentheses."""
        query = self.compiler.compile("$[?(@.age > 25)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)

    def test_both_syntaxes_produce_same_result(self):
        """Test that both syntaxes produce the same SQL."""
        query_without = self.compiler.compile("$[?@.age > 25]")
        query_with = self.compiler.compile("$[?(@.age > 25)]")
        sql_without = str(query_without.compile(compile_kwargs={"literal_binds": True}))
        sql_with = str(query_with.compile(compile_kwargs={"literal_binds": True}))
        self.assertEqual(sql_without, sql_with)

    def test_filter_without_parentheses_string_comparison(self):
        """Test string comparison without parentheses."""
        # jsonpath2 uses double quotes for strings
        query = self.compiler.compile('$[?@.name == "Alice"]')
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.name = 'Alice'", sql)


class TestParenthesesSupport(unittest.TestCase):
    """Test parentheses support in filter expressions."""

    def setUp(self):
        """Set up test schema before each test."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("age", Integer),
            Column("active", Boolean),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_simple_parentheses(self):
        """Test simple parentheses around filter condition."""
        query = self.compiler.compile("$[?(@.age > 25)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)

    def test_nested_parentheses_single_condition(self):
        """Test nested parentheses with single condition."""
        query = self.compiler.compile("$[?((@.age > 25))]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)

    def test_parentheses_with_comparison_operators(self):
        """Test parentheses with different comparison operators."""
        # Greater than
        query = self.compiler.compile("$[?(@.age > 25)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)

        # Less than
        query = self.compiler.compile("$[?(@.age < 65)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age < 65", sql)

        # Equals
        query = self.compiler.compile("$[?(@.age = 30)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age = 30", sql)


class TestLogicalOperatorsSupport(unittest.TestCase):
    """Test logical operators (AND/OR) support."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("age", Integer),
            Column("active", Boolean),
            Column("role", String(50)),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_and_operator(self):
        """Test AND logical operator."""
        query = self.compiler.compile("$[?(@.age > 25 and @.active = true)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.active", sql)

    def test_or_operator(self):
        """Test OR logical operator."""
        query = self.compiler.compile("$[?(@.age < 18 or @.age > 65)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age < 18", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.age > 65", sql)

    def test_and_with_string_comparison(self):
        """Test AND with string comparison."""
        query = self.compiler.compile('$[?(@.name = "Alice" and @.role = "admin")]')
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.name", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.role", sql)

    def test_or_with_different_fields(self):
        """Test OR with different fields."""
        query = self.compiler.compile("$[?(@.active = true or @.role = \"admin\")]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.active", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.role", sql)

    def test_multiple_and_conditions(self):
        """Test multiple AND conditions."""
        query = self.compiler.compile('$[?(@.age > 18 and @.active = true and @.role = "user")]')
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 18", sql)
        self.assertIn("users.active", sql)
        self.assertIn("users.role", sql)
        # Should have two AND operators
        self.assertEqual(sql.count("AND"), 2)


class TestRFC9535LogicalOperatorsNormalization(unittest.TestCase):
    """Test RFC 9535 logical operators (&&, ||) normalization to jsonpath2 syntax."""

    def setUp(self):
        """Set up test schema."""
        self.metadata = MetaData()
        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("age", Integer),
            Column("active", Boolean),
        )

        self.schema = SchemaMetadata(
            tables={"users": self.users_table},
            relationships={},
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_double_ampersand_to_and(self):
        """Test && is converted to AND."""
        query = self.compiler.compile("$[?(@.age > 25 && @.active == true)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age > 25", sql)
        self.assertIn("AND", sql)
        self.assertIn("users.active", sql)

    def test_double_pipe_to_or(self):
        """Test || is converted to OR."""
        query = self.compiler.compile("$[?(@.age < 18 || @.age > 65)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.age < 18", sql)
        self.assertIn("OR", sql)
        self.assertIn("users.age > 65", sql)

    def test_rfc9535_and_jsonpath2_produce_same_result(self):
        """Test that RFC 9535 and jsonpath2 syntaxes produce the same SQL."""
        # RFC 9535 syntax
        query_rfc9535 = self.compiler.compile("$[?(@.age > 25 && @.active == true)]")
        sql_rfc9535 = str(query_rfc9535.compile(compile_kwargs={"literal_binds": True}))
        # jsonpath2 syntax
        query_jsonpath2 = self.compiler.compile("$[?(@.age > 25 and @.active = true)]")
        sql_jsonpath2 = str(query_jsonpath2.compile(compile_kwargs={"literal_binds": True}))
        self.assertEqual(sql_rfc9535, sql_jsonpath2)


class TestNestedPathsInFilters(unittest.TestCase):
    """Test nested paths in filter expressions (e.g., @.orders.total)."""

    def setUp(self):
        """Set up test schema with relationships."""
        self.metadata = MetaData()

        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )

        self.orders_table = Table(
            "orders",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("total", Numeric(10, 2)),
            Column("status", String(50)),
        )

        self.order_items_table = Table(
            "order_items",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("order_id", Integer, ForeignKey("orders.id")),
            Column("price", Numeric(10, 2)),
        )

        self.schema = SchemaMetadata(
            tables={
                "users": self.users_table,
                "orders": self.orders_table,
                "order_items": self.order_items_table,
            },
            relationships={
                "users": {
                    "orders": RelationshipMetadata(
                        target_table="orders",
                        foreign_key="user_id",
                        target_primary_key="id",
                        relationship_type="one-to-many",
                    ),
                },
                "orders": {
                    "items": RelationshipMetadata(
                        target_table="order_items",
                        foreign_key="order_id",
                        target_primary_key="id",
                        relationship_type="one-to-many",
                    ),
                },
            },
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_nested_path_one_level(self):
        """Test nested path @.orders.total with JOIN."""
        query = self.compiler.compile("$[?(@.orders.total > 100)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.id", sql)
        self.assertIn("users.name", sql)
        self.assertIn("JOIN", sql)
        self.assertIn("users.id = orders.user_id", sql)
        self.assertIn("orders.total > 100", sql)

    def test_nested_path_with_string_comparison(self):
        """Test nested path with string comparison."""
        query = self.compiler.compile('$[?(@.orders.status = "completed")]')
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.id = orders.user_id", sql)
        self.assertIn("orders.status", sql)

    def test_nested_path_two_levels(self):
        """Test nested path @.orders.items.price with two JOINs."""
        query = self.compiler.compile("$[?(@.orders.items.price > 50)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.id = orders.user_id", sql)
        self.assertIn("orders.id = order_items.order_id", sql)
        self.assertIn("order_items.price > 50", sql)

    def test_nested_path_with_and_operator(self):
        """Test nested path combined with AND operator."""
        query = self.compiler.compile('$[?(@.orders.total > 100 and @.orders.status = "pending")]')
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("users.id = orders.user_id", sql)
        self.assertIn("orders.total > 100", sql)
        self.assertIn("AND", sql)
        self.assertIn("orders.status", sql)


class TestNestedWildcardsSupport(unittest.TestCase):
    """Test nested wildcards (navigation to related tables) support."""

    def setUp(self):
        """Set up test schema with relationships."""
        self.metadata = MetaData()

        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )

        self.orders_table = Table(
            "orders",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("total", Numeric(10, 2)),
            Column("status", String(50)),
        )

        self.order_items_table = Table(
            "order_items",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("order_id", Integer, ForeignKey("orders.id")),
            Column("price", Numeric(10, 2)),
        )

        self.schema = SchemaMetadata(
            tables={
                "users": self.users_table,
                "orders": self.orders_table,
                "order_items": self.order_items_table,
            },
            relationships={
                "users": {
                    "orders": RelationshipMetadata(
                        target_table="orders",
                        foreign_key="user_id",
                        target_primary_key="id",
                        relationship_type="one-to-many",
                    ),
                },
                "orders": {
                    "items": RelationshipMetadata(
                        target_table="order_items",
                        foreign_key="order_id",
                        target_primary_key="id",
                        relationship_type="one-to-many",
                    ),
                },
            },
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_navigate_to_related_table(self):
        """Test navigation to related table via wildcard."""
        query = self.compiler.compile("$.orders[*]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("orders.id", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("users.id = orders.user_id", sql)

    def test_navigate_and_filter(self):
        """Test navigation to related table with filter."""
        query = self.compiler.compile("$.orders[*][?(@.total > 100)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("users.id = orders.user_id", sql)
        self.assertIn("orders.total > 100", sql)

    def test_navigate_two_levels(self):
        """Test navigation through two relationship levels."""
        query = self.compiler.compile("$.orders[*].items[*]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("FROM users", sql)
        self.assertIn("order_items.id", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("JOIN order_items", sql)

    def test_navigate_two_levels_with_filter(self):
        """Test navigation through two levels with filter."""
        query = self.compiler.compile("$.orders[*].items[*][?(@.price > 50)]")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("JOIN order_items", sql)
        self.assertIn("order_items.price > 50", sql)

    def test_select_field_from_related_table(self):
        """Test selecting specific field from related table."""
        query = self.compiler.compile("$.orders[*].total")
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("FROM users", sql)
        self.assertIn("orders.total", sql)
        self.assertIn("JOIN orders", sql)


if __name__ == "__main__":
    unittest.main()
