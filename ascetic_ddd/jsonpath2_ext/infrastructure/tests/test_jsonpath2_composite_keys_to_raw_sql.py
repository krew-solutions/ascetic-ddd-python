"""
Unit tests for composite keys examples.

Tests JSONPath to Raw SQL compiler with composite primary and foreign keys.
"""
import unittest

from ..jsonpath2_to_raw_sql import (
    JSONPathToRawSQLCompiler, SchemaDef, TableDef, ColumnDef, RelationshipDef,
    RelationType, SQLQuery
)


class TestCompositeKeysExamples(unittest.TestCase):
    """Test cases for composite keys examples."""

    def setUp(self):
        """Set up test schema with composite keys before each test."""
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
                "active": ColumnDef("active", "BOOLEAN", nullable=False),
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
                "level": ColumnDef("level", "INTEGER", nullable=False),
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

    def test_example_access_composite_pk_table(self):
        """Example: Access composite PK table with $[*]"""
        sql = self.compiler.compile("$[*]")

        # Verify SELECT clause
        self.assertIn("SELECT user_roles.*", sql)

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify no JOINs
        self.assertNotIn("JOIN", sql)

        # Verify no WHERE
        self.assertNotIn("WHERE", sql)

    def test_example_select_field_from_composite_pk_table(self):
        """Example: Select field from composite PK table with $.assigned_at"""
        sql = self.compiler.compile("$.assigned_at")

        # Verify specific field selection
        self.assertIn("SELECT user_roles.assigned_at", sql)
        self.assertIn("FROM user_roles", sql)
        self.assertNotIn("JOIN", sql)

    def test_example_filter_on_composite_pk_table(self):
        """Example: Filter on composite PK table with $[?(@.assigned_by = 1)]"""
        sql = self.compiler.compile("$[?(@.assigned_by = 1)]")

        # Verify WHERE clause
        self.assertIn("WHERE user_roles.assigned_by = 1", sql)
        self.assertIn("FROM user_roles", sql)
        self.assertIn("SELECT user_roles.*", sql)

    def test_example_join_via_composite_fk(self):
        """Example: JOIN via composite FK with $.permissions[*]"""
        sql = self.compiler.compile("$.permissions[*]")

        # Verify SELECT from joined table
        self.assertIn("SELECT role_permissions.*", sql)

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify composite FK JOIN with both conditions
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)

        # Verify both conditions are connected with AND
        join_part = sql[sql.find("JOIN role_permissions"):sql.find("WHERE") if "WHERE" in sql else len(sql)]
        self.assertIn("AND", join_part)

    def test_example_join_via_composite_fk_with_filter(self):
        """Example: JOIN via composite FK with filter $.permissions[?(@.permission = "admin")]"""
        sql = self.compiler.compile('$.permissions[?(@.permission = "admin")]')

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify composite FK JOIN
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)

        # Verify WHERE filter
        self.assertIn("WHERE role_permissions.permission = 'admin'", sql)

    def test_example_join_via_single_fk_to_users(self):
        """Example: JOIN via single FK to users with $.user[*]"""
        sql = self.compiler.compile("$.user[*]")

        # Verify SELECT from users
        self.assertIn("SELECT users.*", sql)

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify single-column JOIN (not composite)
        self.assertIn("JOIN users ON user_roles.user_id = users.id", sql)

        # Verify no AND in JOIN (it's a single-column FK)
        join_part = sql[sql.find("JOIN users"):sql.find("WHERE") if "WHERE" in sql else len(sql)]
        self.assertNotIn(" AND ", join_part)

    def test_example_join_via_single_fk_to_roles(self):
        """Example: JOIN via single FK to roles with $.role[*]"""
        sql = self.compiler.compile("$.role[*]")

        # Verify SELECT from roles
        self.assertIn("SELECT roles.*", sql)

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify single-column JOIN
        self.assertIn("JOIN roles ON user_roles.role_id = roles.id", sql)

    def test_example_select_after_composite_fk_join(self):
        """Example: Select after composite FK JOIN with $.permissions.permission"""
        sql = self.compiler.compile("$.permissions.permission")

        # Verify specific field selection from joined table
        self.assertIn("SELECT role_permissions.permission", sql)

        # Verify FROM clause
        self.assertIn("FROM user_roles", sql)

        # Verify composite FK JOIN
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)

    def test_example_complex_filter_on_user(self):
        """Example: Complex filter after single FK JOIN $.user[?(@.active = true)]"""
        sql = self.compiler.compile("$.user[?(@.active = true)]")

        # Verify JOIN to users
        self.assertIn("JOIN users ON user_roles.user_id = users.id", sql)

        # Verify WHERE clause with boolean
        self.assertIn("WHERE users.active = TRUE", sql)

    def test_example_complex_filter_on_role(self):
        """Example: Complex filter on role level $.role[?(@.level > 5)]"""
        sql = self.compiler.compile("$.role[?(@.level > 5)]")

        # Verify JOIN to roles
        self.assertIn("JOIN roles ON user_roles.role_id = roles.id", sql)

        # Verify WHERE clause
        self.assertIn("WHERE roles.level > 5", sql)

    def test_example_multiple_fields_composite_pk(self):
        """Example: Select multiple fields from composite PK table"""
        # Note: This would require extending JSONPath syntax to select multiple fields
        # For now, test selecting one field at a time
        sql1 = self.compiler.compile("$.assigned_at")
        sql2 = self.compiler.compile("$.assigned_by")

        self.assertIn("user_roles.assigned_at", sql1)
        self.assertIn("user_roles.assigned_by", sql2)

    def test_example_composite_fk_field_selection(self):
        """Example: Select specific field after composite FK navigation"""
        sql = self.compiler.compile("$.permissions.granted_at")

        # Verify field selection
        self.assertIn("SELECT role_permissions.granted_at", sql)

        # Verify composite FK JOIN
        self.assertIn("JOIN role_permissions ON", sql)
        self.assertIn("user_roles.user_id = role_permissions.user_id", sql)
        self.assertIn("user_roles.role_id = role_permissions.role_id", sql)


class TestCompositeKeyEdgeCases(unittest.TestCase):
    """Test edge cases for composite keys."""

    def setUp(self):
        """Set up schema with edge cases."""
        # Table with 3-column composite PK
        triple_key_table = TableDef(
            name="triple_key",
            columns={
                "key1": ColumnDef("key1", "INTEGER", nullable=False, primary_key=True),
                "key2": ColumnDef("key2", "INTEGER", nullable=False, primary_key=True),
                "key3": ColumnDef("key3", "INTEGER", nullable=False, primary_key=True),
                "value": ColumnDef("value", "VARCHAR(100)", nullable=True),
            },
            primary_key=("key1", "key2", "key3"),
        )

        # Table with 3-column composite FK
        triple_fk_table = TableDef(
            name="triple_fk",
            columns={
                "id": ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
                "fk1": ColumnDef("fk1", "INTEGER", nullable=False),
                "fk2": ColumnDef("fk2", "INTEGER", nullable=False),
                "fk3": ColumnDef("fk3", "INTEGER", nullable=False),
                "data": ColumnDef("data", "TEXT", nullable=True),
            },
            primary_key="id",
        )

        relationships = {
            "triple_key": {
                "children": RelationshipDef(
                    target_table="triple_fk",
                    foreign_key=("fk1", "fk2", "fk3"),
                    target_primary_key=("fk1", "fk2", "fk3"),
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaDef(
            tables={
                "triple_key": triple_key_table,
                "triple_fk": triple_fk_table,
            },
            relationships=relationships,
            root_table="triple_key",
        )

        self.compiler = JSONPathToRawSQLCompiler(self.schema)

    def test_triple_composite_key_join(self):
        """Test JOIN with 3-column composite key"""
        sql = self.compiler.compile("$.children[*]")

        # Verify all three join conditions
        self.assertIn("triple_key.fk1 = triple_fk.fk1", sql)
        self.assertIn("triple_key.fk2 = triple_fk.fk2", sql)
        self.assertIn("triple_key.fk3 = triple_fk.fk3", sql)

        # Count number of AND operators (should be 2 for 3 conditions)
        join_part = sql[sql.find("JOIN triple_fk"):sql.find("WHERE") if "WHERE" in sql else len(sql)]
        and_count = join_part.count(" AND ")
        self.assertEqual(and_count, 2, "Should have 2 AND operators for 3 join conditions")

    def test_triple_composite_key_with_filter(self):
        """Test triple composite key with filter"""
        sql = self.compiler.compile('$.children[?(@.data = "test")]')

        # Verify JOIN with 3 conditions
        self.assertIn("triple_key.fk1 = triple_fk.fk1", sql)
        self.assertIn("triple_key.fk2 = triple_fk.fk2", sql)
        self.assertIn("triple_key.fk3 = triple_fk.fk3", sql)

        # Verify WHERE clause
        self.assertIn("WHERE triple_fk.data = 'test'", sql)

    def test_triple_composite_pk_field_access(self):
        """Test accessing fields from triple composite PK table"""
        sql = self.compiler.compile("$.value")

        self.assertIn("SELECT triple_key.value", sql)
        self.assertIn("FROM triple_key", sql)


class TestCompositeKeyValidation(unittest.TestCase):
    """Test validation and error handling for composite keys."""

    def test_mismatched_composite_key_lengths(self):
        """Test error when FK and PK have different number of columns"""

        query = SQLQuery("table1")

        # Should raise ValueError when FK has 2 columns but PK has 3
        with self.assertRaises(ValueError) as ctx:
            query.add_join(
                target_table="table2",
                source_table="table1",
                foreign_key=("fk1", "fk2"),
                target_primary_key=("pk1", "pk2", "pk3"),
            )

        self.assertIn("same number of columns", str(ctx.exception))

    def test_composite_key_methods(self):
        """Test helper methods for composite keys"""
        # Test RelationshipDef methods
        rel = RelationshipDef(
            target_table="target",
            foreign_key=("fk1", "fk2"),
            target_primary_key=("pk1", "pk2"),
        )

        self.assertEqual(rel.get_foreign_key_columns(), ["fk1", "fk2"])
        self.assertEqual(rel.get_target_primary_key_columns(), ["pk1", "pk2"])

        # Test with single column (string)
        rel_single = RelationshipDef(
            target_table="target",
            foreign_key="fk1",
            target_primary_key="pk1",
        )

        self.assertEqual(rel_single.get_foreign_key_columns(), ["fk1"])
        self.assertEqual(rel_single.get_target_primary_key_columns(), ["pk1"])

    def test_table_def_composite_pk_methods(self):
        """Test TableDef methods for composite PK"""
        # Composite PK
        table = TableDef(
            name="test",
            columns={
                "col1": ColumnDef("col1", "INTEGER", primary_key=True),
                "col2": ColumnDef("col2", "INTEGER", primary_key=True),
            },
            primary_key=("col1", "col2"),
        )

        self.assertEqual(table.get_primary_key_columns(), ["col1", "col2"])

        # Single PK
        table_single = TableDef(
            name="test",
            columns={"id": ColumnDef("id", "INTEGER", primary_key=True)},
            primary_key="id",
        )

        self.assertEqual(table_single.get_primary_key_columns(), ["id"])


if __name__ == "__main__":
    unittest.main()
