"""
Example usage of JSONPath to Raw SQL compiler.

Demonstrates how to compile JSONPath expressions into raw SQL queries
without ORM or Query Builder dependencies.
"""
from ..infrastructure.jsonpath2_to_raw_sql import (
    JSONPathToRawSQLCompiler, SchemaDef, TableDef, ColumnDef,
    RelationshipDef, RelationType
)


def create_example_schema() -> SchemaDef:
    """Create example schema with users, orders, and products tables."""
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

    schema = SchemaDef(
        tables={
            "users": users_table,
            "orders": orders_table,
            "order_items": order_items_table,
            "products": products_table,
        },
        relationships=relationships,
        root_table="users",
    )

    return schema


def create_composite_keys_schema() -> SchemaDef:
    """Create example schema with composite primary and foreign keys."""
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

    schema = SchemaDef(
        tables={
            "user_roles": user_roles_table,
            "role_permissions": role_permissions_table,
            "users": users_table,
            "roles": roles_table,
        },
        relationships=relationships,
        root_table="user_roles",
    )

    return schema


def main():
    """Run examples."""
    schema = create_example_schema()
    compiler = JSONPathToRawSQLCompiler(schema)

    print("=" * 80)
    print("JSONPath to Raw SQL Compiler Examples")
    print("=" * 80)

    examples = [
        ("Simple field access", "$.name"),
        ("Wildcard (all columns)", "$[*]"),
        ("Filter on age", "$[?(@.age > 18)]"),
        ("Filter with equality", '$[?(@.name = "John")]'),
        ("Navigate to orders", "$.orders[*]"),
        ("Navigate and filter", "$.orders[?(@.total > 100)]"),
        ("Deep navigation", "$.orders.items[*]"),
        ("Very deep navigation", "$.orders.items.product[*]"),
        ("Deep filter", "$.orders.items.product[?(@.price < 50)]"),
        ("Multiple filters", '$[?(@.status = "completed")]'),
    ]

    for idx, (description, jsonpath) in enumerate(examples, 1):
        print(f"\n{idx}. {description}")
        print(f"   JSONPath: {jsonpath}")
        print(f"   SQL:")
        try:
            sql = compiler.compile(jsonpath)
            # Indent SQL for better readability
            for line in sql.split("\n"):
                print(f"      {line}")
        except Exception as e:
            print(f"      Error: {e}")

    print("\n" + "=" * 80)
    print("Real-world example: Find products in high-value orders")
    print("=" * 80)

    real_world_jsonpath = '$.orders[?(@.total > 1000)].items.product[?(@.category = "electronics")]'
    print(f"\nJSONPath: {real_world_jsonpath}")
    print("\nGenerated SQL:")

    try:
        sql = compiler.compile(real_world_jsonpath)
        for line in sql.split("\n"):
            print(f"   {line}")
    except Exception as e:
        print(f"   Error: {e}")
        print("\nNote: This example requires support for multiple filters in path,")
        print("which may need additional implementation.")

    print("\n" + "=" * 80)


def composite_keys_examples():
    """Run examples with composite keys."""
    schema = create_composite_keys_schema()
    compiler = JSONPathToRawSQLCompiler(schema)

    print("\n" + "=" * 80)
    print("Composite Keys Examples")
    print("=" * 80)
    print("\nSchema: user_roles (user_id, role_id) -> role_permissions")
    print("        user_roles.user_id -> users.id")
    print("        user_roles.role_id -> roles.id")

    composite_examples = [
        ("Access composite PK table", "$[*]"),
        ("Select field from composite PK table", "$.assigned_at"),
        ("Filter on composite PK table", "$[?(@.assigned_by = 1)]"),
        ("JOIN via composite FK", "$.permissions[*]"),
        ("JOIN via composite FK with filter", '$.permissions[?(@.permission = "admin")]'),
        ("JOIN via single FK to users", "$.user[*]"),
        ("JOIN via single FK to roles", "$.role[*]"),
        ("Select after composite FK JOIN", "$.permissions.permission"),
    ]

    for idx, (description, jsonpath) in enumerate(composite_examples, 1):
        print(f"\n{idx}. {description}")
        print(f"   JSONPath: {jsonpath}")
        print(f"   SQL:")
        try:
            sql = compiler.compile(jsonpath)
            # Indent SQL for better readability
            for line in sql.split("\n"):
                print(f"      {line}")
        except Exception as e:
            print(f"      Error: {e}")

    print("\n" + "=" * 80)
    print("Real-world composite key example:")
    print("Find all 'admin' permissions for active users")
    print("=" * 80)

    # Note: This would require multiple root tables or more complex navigation
    print("\nJSONPath: $.user.permissions[?(@.permission = \"admin\")]")
    print("Note: Requires navigation from user_roles -> users -> back to permissions")
    print("This demonstrates the complexity of composite key relationships.")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    # Run standard examples
    main()

    # Run composite keys examples
    composite_keys_examples()
