"""
Unit tests for JSONPath RFC 9535 to SQLAlchemy SQL compiler.

Tests the compilation of RFC 9535 compliant JSONPath expressions to SQLAlchemy queries.
"""
import unittest

try:
    from sqlalchemy import MetaData, Table, Column, Integer, String, Numeric, Boolean, ForeignKey
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

if HAS_SQLALCHEMY:
    from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_sqlalchemy_sql import (
        JSONPathToSQLAlchemyCompiler, SchemaMetadata, RelationshipMetadata, RelationType,
        CompilationContext
    )


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestJSONPathToSQLAlchemyCompiler(unittest.TestCase):
    """Test cases for JSONPath RFC 9535 to SQLAlchemy compiler."""

    def setUp(self):
        """Set up test schema before each test."""
        metadata = MetaData()

        # Define tables
        self.users_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            Column('age', Integer),
            Column('active', Boolean, nullable=False),
        )

        self.orders_table = Table(
            'orders',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
            Column('total', Numeric(10, 2), nullable=False),
            Column('status', String(50), nullable=False),
        )

        # Define relationships
        relationships = {
            'users': {
                'orders': RelationshipMetadata(
                    target_table='orders',
                    foreign_key='user_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        # Create schema
        self.schema = SchemaMetadata(
            tables={
                'users': self.users_table,
                'orders': self.orders_table,
            },
            relationships=relationships,
            root_table='users',
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_simple_field_access(self):
        """Test simple field access."""
        query = self.compiler.compile("$.name")
        sql = str(query)
        self.assertIn("users.name", sql)
        self.assertIn("FROM users", sql)

    def test_wildcard_selection(self):
        """Test wildcard selection."""
        query = self.compiler.compile("$[*]")
        sql = str(query)
        # Should select all columns
        self.assertIn("users.id", sql)
        self.assertIn("users.name", sql)
        self.assertIn("users.age", sql)
        self.assertIn("users.active", sql)

    def test_filter_greater_than(self):
        """Test filter with > operator."""
        query = self.compiler.compile("$[?@.age > 18]")
        sql = str(query)
        self.assertIn("FROM users", sql)
        self.assertIn("WHERE users.age >", sql)

    def test_filter_equality(self):
        """Test filter with == operator (RFC 9535 standard)."""
        query = self.compiler.compile("$[?@.age == 30]")
        sql = str(query)
        self.assertIn("WHERE users.age =", sql)

    def test_filter_string_value(self):
        """Test filter with string value."""
        query = self.compiler.compile("$[?@.name == 'John']")
        sql = str(query)
        self.assertIn("WHERE users.name =", sql)

    def test_filter_logical_and(self):
        """Test filter with AND operator (&&)."""
        query = self.compiler.compile("$[?@.age > 25 && @.active == true]")
        sql = str(query)
        self.assertIn("WHERE users.age >", sql)
        self.assertIn("AND users.active =", sql)

    def test_filter_logical_or(self):
        """Test filter with OR operator (||)."""
        query = self.compiler.compile("$[?@.age < 18 || @.age > 65]")
        sql = str(query)
        self.assertIn("WHERE users.age <", sql)
        self.assertIn("OR users.age >", sql)

    def test_filter_logical_not(self):
        """Test filter with NOT operator (!)."""
        query = self.compiler.compile("$[?!(@.active == false)]")
        sql = str(query)
        self.assertIn("WHERE users.active !=", sql)

    def test_navigate_to_relationship(self):
        """Test navigation to related table."""
        query = self.compiler.compile("$.orders[*]")
        sql = str(query)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("orders.user_id = users.id", sql)

    def test_navigate_with_filter(self):
        """Test navigation with filter."""
        query = self.compiler.compile("$.orders[?@.total > 100]")
        sql = str(query)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("WHERE orders.total >", sql)

    def test_comparison_operators(self):
        """Test all comparison operators."""
        test_cases = [
            ("$[?@.age > 18]", "users.age >"),
            ("$[?@.age < 65]", "users.age <"),
            ("$[?@.age >= 18]", "users.age >="),
            ("$[?@.age <= 65]", "users.age <="),
            ("$[?@.age != 30]", "users.age !="),
        ]

        for jsonpath, expected_sql_fragment in test_cases:
            query = self.compiler.compile(jsonpath)
            sql = str(query)
            self.assertIn(expected_sql_fragment, sql)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestNestedWildcardsInFilters(unittest.TestCase):
    """Test cases for nested wildcards in filter expressions."""

    def setUp(self):
        """Set up test schema with nested relationships."""
        metadata = MetaData()

        # Define tables: categories -> items
        self.categories_table = Table(
            'categories',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
        )

        self.items_table = Table(
            'items',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('category_id', Integer, ForeignKey('categories.id'), nullable=False),
            Column('name', String(100), nullable=False),
            Column('price', Numeric(10, 2), nullable=False),
        )

        # Define relationships
        relationships = {
            'categories': {
                'items': RelationshipMetadata(
                    target_table='items',
                    foreign_key='category_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        # Create schema
        self.schema = SchemaMetadata(
            tables={
                'categories': self.categories_table,
                'items': self.items_table,
            },
            relationships=relationships,
            root_table='categories',
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_nested_wildcard_simple(self):
        """Test nested wildcard with simple filter."""
        query = self.compiler.compile("$[*][?@.items[*][?@.price > 100]]")
        sql = str(query)

        # Check main SELECT
        self.assertIn("FROM categories", sql)

        # Check EXISTS subquery
        self.assertIn("EXISTS", sql)
        self.assertIn("items", sql)
        self.assertIn("items.category_id = categories.id", sql)
        self.assertIn("items.price >", sql)

    def test_nested_wildcard_with_logical_operators(self):
        """Test nested wildcard with logical operators in inner filter."""
        query = self.compiler.compile("$[*][?@.items[*][?@.price > 50 && @.price < 200]]")
        sql = str(query)

        self.assertIn("EXISTS", sql)
        self.assertIn("items.price >", sql)
        self.assertIn("items.price <", sql)
        self.assertIn("AND", sql)

    def test_nested_wildcard_with_string_comparison(self):
        """Test nested wildcard with string comparison."""
        query = self.compiler.compile("$[*][?@.items[*][?@.name == 'Laptop']]")
        sql = str(query)

        self.assertIn("EXISTS", sql)
        self.assertIn("items.name =", sql)

    def test_nested_wildcard_combined_with_direct_filter(self):
        """Test nested wildcard combined with direct field filter."""
        query = self.compiler.compile("$[?@.name == 'Electronics'][?@.items[*][?@.price > 500]]")
        sql = str(query)

        # Should have both conditions
        self.assertIn("categories.name =", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price >", sql)

    def test_nested_wildcard_not_operator(self):
        """Test nested wildcard with NOT operator."""
        query = self.compiler.compile("$[*][?!(@.items[*][?@.price > 1000])]")
        sql = str(query)

        self.assertIn("NOT", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price >", sql)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestParenthesesAndComplexFilters(unittest.TestCase):
    """Test cases for parentheses in filter expressions."""

    def setUp(self):
        """Set up test schema before each test."""
        metadata = MetaData()

        self.users_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            Column('age', Integer),
            Column('active', Boolean, nullable=False),
            Column('status', String(50)),
        )

        self.orders_table = Table(
            'orders',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
            Column('total', Numeric(10, 2), nullable=False),
        )

        relationships = {
            'users': {
                'orders': RelationshipMetadata(
                    target_table='orders',
                    foreign_key='user_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaMetadata(
            tables={'users': self.users_table, 'orders': self.orders_table},
            relationships=relationships,
            root_table='users',
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_parentheses_simple_grouping(self):
        """Test parentheses for simple grouping."""
        query = self.compiler.compile("$[?@.age >= 18 && @.age <= 65]")
        sql = str(query)

        self.assertIn("users.age >=", sql)
        self.assertIn("users.age <=", sql)
        self.assertIn("AND", sql)

    def test_parentheses_complex_logic(self):
        """Test parentheses with complex logical expressions."""
        query = self.compiler.compile("$[?@.age > 25 || @.age < 18]")
        sql = str(query)

        self.assertIn("users.age >", sql)
        self.assertIn("users.age <", sql)
        self.assertIn("OR", sql)

    def test_parentheses_with_not(self):
        """Test parentheses combined with NOT operator."""
        query = self.compiler.compile("$[?!(@.age < 18 || @.age > 65)]")
        sql = str(query)

        self.assertIn("NOT", sql)
        self.assertIn("users.age <", sql)
        self.assertIn("users.age >", sql)
        self.assertIn("OR", sql)

    def test_parentheses_nested_with_wildcard_filters(self):
        """Test parentheses combined with nested wildcard filters."""
        query = self.compiler.compile(
            "$[?(@.age > 18 && @.active == true) && @.orders[*][?@.total > 100]]"
        )
        sql = str(query)

        self.assertIn("users.age >", sql)
        self.assertIn("users.active", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("orders.total >", sql)

    def test_multiple_parentheses_levels(self):
        """Test multiple levels of parentheses."""
        query = self.compiler.compile(
            "$[?((@.age >= 18 && @.age <= 25) || (@.age >= 60 && @.age <= 70)) && @.active == true]"
        )
        sql = str(query)

        self.assertIn("users.age >=", sql)
        self.assertIn("users.age <=", sql)
        self.assertIn("users.active", sql)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestDeepNestedWildcards(unittest.TestCase):
    """Test cases for deeply nested wildcard patterns."""

    def setUp(self):
        """Set up test schema with 3-level nesting."""
        metadata = MetaData()

        # stores -> categories -> items
        self.stores_table = Table(
            'stores',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
        )

        self.categories_table = Table(
            'categories',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('store_id', Integer, ForeignKey('stores.id'), nullable=False),
            Column('name', String(100), nullable=False),
        )

        self.items_table = Table(
            'items',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('category_id', Integer, ForeignKey('categories.id'), nullable=False),
            Column('price', Numeric(10, 2), nullable=False),
            Column('quantity', Integer, nullable=False),
        )

        relationships = {
            'stores': {
                'categories': RelationshipMetadata(
                    target_table='categories',
                    foreign_key='store_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            'categories': {
                'items': RelationshipMetadata(
                    target_table='items',
                    foreign_key='category_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaMetadata(
            tables={
                'stores': self.stores_table,
                'categories': self.categories_table,
                'items': self.items_table,
            },
            relationships=relationships,
            root_table='stores',
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_two_level_nesting(self):
        """Test 2-level nested wildcards: stores -> categories -> items."""
        query = self.compiler.compile(
            "$[*][?@.categories[*][?@.items[*][?@.price > 100]]]"
        )
        sql = str(query)

        # Should have 2 nested EXISTS
        self.assertEqual(sql.count("EXISTS"), 2)
        self.assertIn("stores", sql)
        self.assertIn("categories", sql)
        self.assertIn("items", sql)
        self.assertIn("items.price >", sql)

    def test_two_level_nesting_with_multiple_conditions(self):
        """Test 2-level nesting with conditions at multiple levels."""
        query = self.compiler.compile(
            "$[?@.name == 'MainStore'][?@.categories[*][?@.items[*][?@.price > 50 && @.quantity > 5]]]"
        )
        sql = str(query)

        # Should filter store by name
        self.assertIn("stores.name =", sql)
        # Should have nested EXISTS
        self.assertIn("EXISTS", sql)
        # Should have item filters
        self.assertIn("items.price >", sql)
        self.assertIn("items.quantity >", sql)

    def test_navigation_with_nested_filter(self):
        """Test navigation to categories, then filter by items."""
        query = self.compiler.compile(
            "$.categories[*][?@.items[*][?@.price > 200]]"
        )
        sql = str(query)

        # Should join to categories
        self.assertIn("categories", sql)
        # Should have EXISTS for items filter
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price >", sql)

    def test_nested_wildcard_with_and_conditions(self):
        """Test nested wildcards with AND conditions."""
        query = self.compiler.compile(
            "$[*][?@.categories[*][?@.items[*][?@.price > 50 && @.price < 200]]]"
        )
        sql = str(query)

        self.assertIn("EXISTS", sql)
        self.assertIn("items.price >", sql)
        self.assertIn("items.price <", sql)
        self.assertIn("AND", sql)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestNestedPaths(unittest.TestCase):
    """Test cases for nested paths in filter expressions (e.g., @.orders.total)."""

    def setUp(self):
        """Set up test schema with relationships."""
        metadata = MetaData()

        self.users_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            Column('age', Integer),
        )

        self.orders_table = Table(
            'orders',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
            Column('total', Numeric(10, 2), nullable=False),
            Column('status', String(50), nullable=False),
        )

        self.order_items_table = Table(
            'order_items',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('order_id', Integer, ForeignKey('orders.id'), nullable=False),
            Column('price', Numeric(10, 2), nullable=False),
        )

        relationships = {
            'users': {
                'orders': RelationshipMetadata(
                    target_table='orders',
                    foreign_key='user_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            'orders': {
                'order_items': RelationshipMetadata(
                    target_table='order_items',
                    foreign_key='order_id',
                    target_primary_key='id',
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaMetadata(
            tables={
                'users': self.users_table,
                'orders': self.orders_table,
                'order_items': self.order_items_table,
            },
            relationships=relationships,
            root_table='users',
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_nested_path_simple(self):
        """Test simple nested path: @.orders.total."""
        query = self.compiler.compile("$[?@.orders.total > 100]")
        sql = str(query)

        self.assertIn("users", sql)
        self.assertIn("orders", sql)
        self.assertIn("JOIN", sql)
        self.assertIn("orders.total >", sql)

    def test_nested_path_multiple_conditions(self):
        """Test nested path with multiple conditions on same relationship."""
        query = self.compiler.compile("$[?@.orders.total > 100 && @.orders.status == 'completed']")
        sql = str(query)

        self.assertIn("JOIN", sql)
        self.assertIn("orders.total >", sql)
        self.assertIn("orders.status =", sql)

    def test_nested_path_mixed_with_direct_field(self):
        """Test nested path combined with direct field access."""
        query = self.compiler.compile("$[?@.age > 18 && @.orders.total > 50]")
        sql = str(query)

        self.assertIn("users.age >", sql)
        self.assertIn("JOIN", sql)
        self.assertIn("orders.total >", sql)

    def test_nested_path_two_levels(self):
        """Test two-level nested path from orders context."""
        orders_schema = SchemaMetadata(
            tables=self.schema.tables,
            relationships=self.schema.relationships,
            root_table='orders',
        )
        compiler = JSONPathToSQLAlchemyCompiler(orders_schema)

        query = compiler.compile("$[?@.order_items.price > 50]")
        sql = str(query)

        self.assertIn("JOIN", sql)
        self.assertIn("order_items.price >", sql)

    def test_nested_path_or_condition(self):
        """Test nested path with OR condition."""
        query = self.compiler.compile("$[?@.orders.total > 1000 || @.orders.status == 'vip']")
        sql = str(query)

        self.assertIn("JOIN", sql)
        self.assertIn("orders.total >", sql)
        self.assertIn("orders.status =", sql)
        self.assertIn("OR", sql)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
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
        self.assertEqual(rel.relationship_type, RelationType.ONE_TO_MANY)

    def test_custom_values(self):
        """Test custom values for RelationshipMetadata."""
        rel = RelationshipMetadata(
            target_table="products",
            foreign_key="product_id",
            target_primary_key="product_id",
            relationship_type=RelationType.MANY_TO_ONE,
        )

        self.assertEqual(rel.target_table, "products")
        self.assertEqual(rel.foreign_key, "product_id")
        self.assertEqual(rel.target_primary_key, "product_id")
        self.assertEqual(rel.relationship_type, RelationType.MANY_TO_ONE)


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
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


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
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


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
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

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_nonexistent_column(self):
        """Test accessing non-existent column."""
        with self.assertRaises(ValueError) as cm:
            self.compiler.compile("$.nonexistent")

        self.assertIn("not found", str(cm.exception))

    def test_invalid_jsonpath_syntax(self):
        """Test invalid JSONPath syntax."""
        with self.assertRaises(Exception):
            self.compiler.compile("$[invalid")


if __name__ == "__main__":
    unittest.main()
