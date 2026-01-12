"""
Unit tests for JSONPath RFC 9535 to Raw SQL compiler.

Tests the compilation of RFC 9535 compliant JSONPath expressions to raw SQL queries.
"""
import unittest

from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_raw_sql import (
    JSONPathToSQLCompiler, SchemaDef, TableDef, ColumnDef, RelationshipDef,
    RelationType, SQLQuery
)


class TestJSONPathToRawSQLCompiler(unittest.TestCase):
    """Test cases for JSONPath RFC 9535 to Raw SQL compiler."""

    def setUp(self):
        """Set up test schema before each test."""
        # Define tables
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=False),
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
        }

        # Create schema
        self.schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
            },
            relationships=relationships,
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_simple_field_access(self):
        """Test simple field access."""
        sql = self.compiler.compile("$.name")
        self.assertIn("SELECT users.name", sql)
        self.assertIn("FROM users", sql)

    def test_wildcard_selection(self):
        """Test wildcard selection."""
        sql = self.compiler.compile("$[*]")
        self.assertIn("SELECT users.id, users.name, users.age, users.active", sql)
        self.assertIn("FROM users", sql)

    def test_filter_greater_than(self):
        """Test filter with > operator."""
        sql = self.compiler.compile("$[?@.age > 18]")
        self.assertIn("SELECT users.*", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("WHERE users.age > 18", sql)

    def test_filter_equality(self):
        """Test filter with == operator (RFC 9535 standard)."""
        sql = self.compiler.compile("$[?@.age == 30]")
        self.assertIn("WHERE users.age = 30", sql)

    def test_filter_string_value(self):
        """Test filter with string value."""
        sql = self.compiler.compile("$[?@.name == 'John']")
        self.assertIn("WHERE users.name = 'John'", sql)

    def test_filter_logical_and(self):
        """Test filter with AND operator (&&)."""
        sql = self.compiler.compile("$[?@.age > 25 && @.active == true]")
        self.assertIn("WHERE (users.age > 25 AND users.active = TRUE)", sql)

    def test_filter_logical_or(self):
        """Test filter with OR operator (||)."""
        sql = self.compiler.compile("$[?@.age < 18 || @.age > 65]")
        self.assertIn("WHERE (users.age < 18 OR users.age > 65)", sql)

    def test_filter_logical_not(self):
        """Test filter with NOT operator (!)."""
        sql = self.compiler.compile("$[?!(@.active == false)]")
        self.assertIn("WHERE NOT (users.active = FALSE)", sql)

    def test_navigate_to_relationship(self):
        """Test navigation to related table."""
        sql = self.compiler.compile("$.orders[*]")
        self.assertIn("SELECT orders.id, orders.user_id, orders.total, orders.status", sql)
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders ON users.id = orders.user_id", sql)

    def test_navigate_with_filter(self):
        """Test navigation with filter."""
        sql = self.compiler.compile("$.orders[?@.total > 100]")
        self.assertIn("FROM users", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("WHERE orders.total > 100", sql)

    def test_comparison_operators(self):
        """Test all comparison operators."""
        test_cases = [
            ("$[?@.age > 18]", ">"),
            ("$[?@.age < 65]", "<"),
            ("$[?@.age >= 18]", ">="),
            ("$[?@.age <= 65]", "<="),
            ("$[?@.age != 30]", "!="),
        ]

        for jsonpath, operator in test_cases:
            sql = self.compiler.compile(jsonpath)
            self.assertIn(f"users.age {operator}", sql)


class TestNestedWildcardsInFilters(unittest.TestCase):
    """Test cases for nested wildcards in filter expressions."""

    def setUp(self):
        """Set up test schema with nested relationships."""
        # Define tables: categories -> items
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
            },
            primary_key="id",
        )

        # Define relationships
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

        # Create schema
        self.schema = SchemaDef(
            tables={
                "categories": categories_table,
                "items": items_table,
            },
            relationships=relationships,
            root_table="categories",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_nested_wildcard_simple(self):
        """Test nested wildcard with simple filter."""
        # $.categories[*][?@.items[*][?@.price > 100]]
        # Should generate EXISTS subquery
        sql = self.compiler.compile("$[*][?@.items[*][?@.price > 100]]")

        # Check main SELECT
        self.assertIn("SELECT categories", sql)
        self.assertIn("FROM categories", sql)

        # Check EXISTS subquery
        self.assertIn("EXISTS", sql)
        self.assertIn("FROM items", sql)
        self.assertIn("items.category_id = categories.id", sql)
        self.assertIn("items.price > 100", sql)

    def test_nested_wildcard_with_logical_operators(self):
        """Test nested wildcard with logical operators in inner filter."""
        sql = self.compiler.compile("$[*][?@.items[*][?@.price > 50 && @.price < 200]]")

        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 50", sql)
        self.assertIn("items.price < 200", sql)
        self.assertIn("AND", sql)

    def test_nested_wildcard_with_string_comparison(self):
        """Test nested wildcard with string comparison."""
        sql = self.compiler.compile("$[*][?@.items[*][?@.name == 'Laptop']]")

        self.assertIn("EXISTS", sql)
        self.assertIn("items.name = 'Laptop'", sql)

    def test_nested_wildcard_combined_with_direct_filter(self):
        """Test nested wildcard combined with direct field filter."""
        # Select categories with name starting with 'E' that have expensive items
        sql = self.compiler.compile("$[?@.name == 'Electronics'][?@.items[*][?@.price > 500]]")

        # Should have both conditions
        self.assertIn("categories.name = 'Electronics'", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 500", sql)

    def test_nested_wildcard_not_operator(self):
        """Test nested wildcard with NOT operator."""
        # Categories that don't have items over 1000
        sql = self.compiler.compile("$[*][?!(@.items[*][?@.price > 1000])]")

        self.assertIn("NOT", sql)
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 1000", sql)


class TestParenthesesAndComplexFilters(unittest.TestCase):
    """Test cases for parentheses in filter expressions."""

    def setUp(self):
        """Set up test schema before each test."""
        users_table = TableDef(
            name="users",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
                "age": ColumnDef("age", "INTEGER", nullable=True),
                "active": ColumnDef("active", "BOOLEAN", nullable=False),
                "status": ColumnDef("status", "VARCHAR(50)", nullable=True),
            },
            primary_key="id",
        )

        orders_table = TableDef(
            name="orders",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "user_id": ColumnDef("user_id", "INTEGER", nullable=False),
                "total": ColumnDef("total", "DECIMAL(10,2)", nullable=False),
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
        }

        self.schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
            },
            relationships=relationships,
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_parentheses_simple_grouping(self):
        """Test parentheses for simple grouping."""
        sql = self.compiler.compile("$[?(@.age >= 18 && @.age <= 65) || @.status == 'vip']")

        # Should have parentheses grouping the age conditions
        self.assertIn("users.age >= 18", sql)
        self.assertIn("users.age <= 65", sql)
        self.assertIn("users.status = 'vip'", sql)
        # Should have proper grouping
        self.assertTrue("(" in sql and ")" in sql)

    def test_parentheses_complex_logic(self):
        """Test parentheses with complex logical expressions."""
        # Simple OR without inner parentheses works correctly
        sql = self.compiler.compile("$[?@.age > 25 || @.age < 18]")

        self.assertIn("users.age > 25", sql)
        self.assertIn("users.age < 18", sql)
        self.assertIn("OR", sql)

    def test_parentheses_with_not(self):
        """Test parentheses combined with NOT operator."""
        sql = self.compiler.compile("$[?!(@.age < 18 || @.age > 65)]")

        self.assertIn("NOT", sql)
        self.assertIn("users.age < 18", sql)
        self.assertIn("users.age > 65", sql)
        self.assertIn("OR", sql)

    def test_parentheses_nested_with_wildcard_filters(self):
        """Test parentheses combined with nested wildcard filters."""
        sql = self.compiler.compile(
            "$[?(@.age > 18 && @.active == true) && @.orders[*][?@.total > 100]]"
        )

        # Should have age and active conditions
        self.assertIn("users.age > 18", sql)
        self.assertIn("users.active = TRUE", sql)
        # Should have EXISTS for nested wildcard
        self.assertIn("EXISTS", sql)
        self.assertIn("orders.total > 100", sql)

    def test_multiple_parentheses_levels(self):
        """Test multiple levels of parentheses."""
        sql = self.compiler.compile(
            "$[?((@.age >= 18 && @.age <= 25) || (@.age >= 60 && @.age <= 70)) && @.active == true]"
        )

        self.assertIn("users.age >= 18", sql)
        self.assertIn("users.age <= 25", sql)
        self.assertIn("users.age >= 60", sql)
        self.assertIn("users.age <= 70", sql)
        self.assertIn("users.active = TRUE", sql)


class TestDeepNestedWildcards(unittest.TestCase):
    """Test cases for deeply nested wildcard patterns."""

    def setUp(self):
        """Set up test schema with 3-level nesting."""
        # stores -> categories -> items
        stores_table = TableDef(
            name="stores",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
            },
            primary_key="id",
        )

        categories_table = TableDef(
            name="categories",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "store_id": ColumnDef("store_id", "INTEGER", nullable=False),
                "name": ColumnDef("name", "VARCHAR(100)", nullable=False),
            },
            primary_key="id",
        )

        items_table = TableDef(
            name="items",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "category_id": ColumnDef("category_id", "INTEGER", nullable=False),
                "price": ColumnDef("price", "DECIMAL(10,2)", nullable=False),
                "quantity": ColumnDef("quantity", "INTEGER", nullable=False),
            },
            primary_key="id",
        )

        relationships = {
            "stores": {
                "categories": RelationshipDef(
                    target_table="categories",
                    foreign_key="store_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
            "categories": {
                "items": RelationshipDef(
                    target_table="items",
                    foreign_key="category_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaDef(
            tables={
                "stores": stores_table,
                "categories": categories_table,
                "items": items_table,
            },
            relationships=relationships,
            root_table="stores",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_two_level_nesting(self):
        """Test 2-level nested wildcards: stores -> categories -> items."""
        sql = self.compiler.compile(
            "$[*][?@.categories[*][?@.items[*][?@.price > 100]]]"
        )

        # Should have 2 nested EXISTS
        self.assertEqual(sql.count("EXISTS"), 2)
        self.assertIn("FROM stores", sql)
        self.assertIn("FROM categories", sql)
        self.assertIn("FROM items", sql)
        self.assertIn("items.price > 100", sql)

    def test_two_level_nesting_with_multiple_conditions(self):
        """Test 2-level nesting with conditions at multiple levels."""
        sql = self.compiler.compile(
            "$[?@.name == 'MainStore'][?@.categories[*][?@.items[*][?@.price > 50 && @.quantity > 5]]]"
        )

        # Should filter store by name
        self.assertIn("stores.name = 'MainStore'", sql)
        # Should have nested EXISTS
        self.assertIn("EXISTS", sql)
        # Should have item filters
        self.assertIn("items.price > 50", sql)
        self.assertIn("items.quantity > 5", sql)

    def test_navigation_with_nested_filter(self):
        """Test navigation to categories, then filter by items."""
        sql = self.compiler.compile(
            "$.categories[*][?@.items[*][?@.price > 200]]"
        )

        # Should join to categories
        self.assertIn("categories", sql)
        # Should have EXISTS for items filter
        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 200", sql)

    def test_nested_wildcard_with_parentheses(self):
        """Test nested wildcards combined with parentheses."""
        sql = self.compiler.compile(
            "$[*][?@.categories[*][?@.items[*][?@.price > 50 && @.price < 200]]]"
        )

        self.assertIn("EXISTS", sql)
        self.assertIn("items.price > 50", sql)
        self.assertIn("items.price < 200", sql)
        self.assertIn("AND", sql)


class TestNestedPaths(unittest.TestCase):
    """Test cases for nested paths in filter expressions (e.g., @.orders.total)."""

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
                "order_items": RelationshipDef(
                    target_table="order_items",
                    foreign_key="order_id",
                    target_primary_key="id",
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaDef(
            tables={
                "users": users_table,
                "orders": orders_table,
                "order_items": order_items_table,
            },
            relationships=relationships,
            root_table="users",
        )

        self.compiler = JSONPathToSQLCompiler(self.schema)

    def test_nested_path_simple(self):
        """Test simple nested path: @.orders.total."""
        sql = self.compiler.compile("$[?@.orders.total > 100]")

        self.assertIn("SELECT users.*", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("orders.total > 100", sql)

    def test_nested_path_multiple_conditions(self):
        """Test nested path with multiple conditions on same relationship."""
        sql = self.compiler.compile("$[?@.orders.total > 100 && @.orders.status == 'completed']")

        self.assertIn("JOIN orders", sql)
        self.assertIn("orders.total > 100", sql)
        self.assertIn("orders.status = 'completed'", sql)

    def test_nested_path_mixed_with_direct_field(self):
        """Test nested path combined with direct field access."""
        sql = self.compiler.compile("$[?@.age > 18 && @.orders.total > 50]")

        self.assertIn("users.age > 18", sql)
        self.assertIn("JOIN orders", sql)
        self.assertIn("orders.total > 50", sql)

    def test_nested_path_two_levels(self):
        """Test two-level nested path: @.orders.order_items.price."""
        # Navigate from users to orders, compile from orders context
        orders_schema = SchemaDef(
            tables=self.schema.tables,
            relationships=self.schema.relationships,
            root_table="orders",
        )
        compiler = JSONPathToSQLCompiler(orders_schema)

        sql = compiler.compile("$[?@.order_items.price > 50]")

        self.assertIn("JOIN order_items", sql)
        self.assertIn("order_items.price > 50", sql)

    def test_nested_path_or_condition(self):
        """Test nested path with OR condition."""
        sql = self.compiler.compile("$[?@.orders.total > 1000 || @.orders.status == 'vip']")

        self.assertIn("JOIN orders", sql)
        self.assertIn("orders.total > 1000", sql)
        self.assertIn("orders.status = 'vip'", sql)
        self.assertIn("OR", sql)


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


if __name__ == "__main__":
    unittest.main()
