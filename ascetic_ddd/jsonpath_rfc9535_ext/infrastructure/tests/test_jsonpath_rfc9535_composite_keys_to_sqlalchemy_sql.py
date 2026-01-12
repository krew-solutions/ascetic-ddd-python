"""
Unit tests for composite keys examples with SQLAlchemy (RFC 9535).

Tests JSONPath to SQLAlchemy compiler with composite primary and foreign keys.
"""
import unittest

try:
    from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, Text
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

if HAS_SQLALCHEMY:
    from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_sqlalchemy_sql import (
        JSONPathToSQLAlchemyCompiler, SchemaMetadata, RelationshipMetadata, RelationType,
        CompilationContext
    )


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestCompositeKeysExamples(unittest.TestCase):
    """Test cases for composite keys with SQLAlchemy."""

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
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
                "user": RelationshipMetadata(
                    target_table="users",
                    foreign_key="user_id",
                    target_primary_key="id",
                    relationship_type=RelationType.MANY_TO_ONE,
                ),
                "role": RelationshipMetadata(
                    target_table="roles",
                    foreign_key="role_id",
                    target_primary_key="id",
                    relationship_type=RelationType.MANY_TO_ONE,
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

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

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
        """Test filter on composite PK table with $[?@.assigned_by == 1]"""
        query = self.compiler.compile("$[?@.assigned_by == 1]")

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
        """Test JOIN via composite FK with filter $.permissions[?@.permission == 'admin']"""
        query = self.compiler.compile("$.permissions[?@.permission == 'admin']")

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
        """Test complex filter after single FK JOIN $.user[?@.active == true]"""
        query = self.compiler.compile("$.user[?@.active == true]")

        sql = str(query)
        self.assertIn("users", sql)
        self.assertIn("active", sql)

    def test_complex_filter_on_role(self):
        """Test complex filter on role level $.role[?@.level > 5]"""
        query = self.compiler.compile("$.role[?@.level > 5]")

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


@unittest.skipIf(not HAS_SQLALCHEMY, "SQLAlchemy not installed")
class TestCompositeKeyEdgeCases(unittest.TestCase):
    """Test edge cases for composite keys with SQLAlchemy."""

    def setUp(self):
        """Set up schema with edge cases."""
        self.metadata = MetaData()

        # Table with 3-column composite PK
        self.triple_key_table = Table(
            "triple_key",
            self.metadata,
            Column("key1", Integer, primary_key=True),
            Column("key2", Integer, primary_key=True),
            Column("key3", Integer, primary_key=True),
            Column("fk1", Integer),  # For ONE_TO_MANY join
            Column("fk2", Integer),  # For ONE_TO_MANY join
            Column("fk3", Integer),  # For ONE_TO_MANY join
            Column("value", String),
        )

        # Table with 3-column composite FK
        self.triple_fk_table = Table(
            "triple_fk",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("fk1", Integer),
            Column("fk2", Integer),
            Column("fk3", Integer),
            Column("data", Text),
        )

        relationships = {
            "triple_key": {
                "children": RelationshipMetadata(
                    target_table="triple_fk",
                    foreign_key=("fk1", "fk2", "fk3"),
                    target_primary_key=("fk1", "fk2", "fk3"),
                    relationship_type=RelationType.ONE_TO_MANY,
                ),
            },
        }

        self.schema = SchemaMetadata(
            tables={
                "triple_key": self.triple_key_table,
                "triple_fk": self.triple_fk_table,
            },
            relationships=relationships,
            root_table="triple_key",
        )

        self.compiler = JSONPathToSQLAlchemyCompiler(self.schema)

    def test_triple_composite_key_join(self):
        """Test JOIN with 3-column composite key"""
        query = self.compiler.compile("$.children[*]")

        sql = str(query)
        # Verify all three join conditions
        self.assertIn("fk1", sql)
        self.assertIn("fk2", sql)
        self.assertIn("fk3", sql)
        self.assertIn("JOIN", sql.upper())

    def test_triple_composite_key_with_filter(self):
        """Test triple composite key with filter"""
        query = self.compiler.compile("$.children[?@.data == 'test']")

        sql = str(query)
        # Verify JOIN with 3 conditions
        self.assertIn("fk1", sql)
        self.assertIn("fk2", sql)
        self.assertIn("fk3", sql)

        # Verify WHERE clause
        self.assertIn("WHERE", sql.upper())
        self.assertIn("data", sql)

    def test_triple_composite_pk_field_access(self):
        """Test accessing fields from triple composite PK table"""
        query = self.compiler.compile("$.value")

        sql = str(query)
        self.assertIn("triple_key.value", sql)
        self.assertIn("FROM triple_key", sql)


if __name__ == "__main__":
    unittest.main()
