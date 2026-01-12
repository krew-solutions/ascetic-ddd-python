"""
Example usage of JSONPath RFC 9535 to Raw SQL compiler.

Demonstrates how to compile RFC 9535 compliant JSONPath expressions
into raw SQL queries without ORM or Query Builder dependencies.
"""
from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_raw_sql import (
    JSONPathToSQLCompiler, SchemaDef, TableDef, ColumnDef,
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
            "order_items": RelationshipDef(
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
            "products": products_table,
            "order_items": order_items_table,
        },
        relationships=relationships,
        root_table="users",
    )

    return schema


def main():
    """Run examples."""
    schema = create_example_schema()
    compiler = JSONPathToSQLCompiler(schema)

    print("=" * 80)
    print("JSONPath RFC 9535 to Raw SQL Compiler Examples")
    print("=" * 80)

    examples = [
        ("Simple field access", "$.name"),
        ("Wildcard (all columns)", "$[*]"),
        ("Filter on age", "$[?@.age > 18]"),
        ("Filter with equality", "$[?@.name == 'John']"),
        ("Navigate to orders", "$.orders[*]"),
        ("Navigate and filter", "$.orders[?@.total > 100]"),
        ("Logical AND", "$[?@.age > 25 && @.active == true]"),
        ("Logical OR", "$[?@.age < 18 || @.age > 65]"),
        ("Logical NOT", "$[?!(@.active == false)]"),
        ("Parentheses for grouping", "$[?(@.age >= 18 && @.age <= 65) || @.active == true]"),
        # Nested paths - access fields through relationships
        ("Nested path: filter by related field", "$[?@.orders.total > 100]"),
        ("Nested path: multiple conditions", "$[?@.orders.total > 100 && @.orders.status == 'completed']"),
        ("Nested path: mixed with direct field", "$[?@.age > 18 && @.orders.total > 50]"),
        # Nested wildcards - filter parent by child collection
        ("Nested wildcard: orders with expensive items", "$.orders[*][?@.order_items[*][?@.price > 100]]"),
        ("Nested wildcard with filter", "$[?@.orders[*][?@.order_items[*][?@.quantity > 2]]]"),
        ("Complex nested with AND", "$.orders[*][?@.order_items[*][?@.price > 50 && @.quantity > 1]]"),
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
    print("RFC 9535 Compliance Examples")
    print("=" * 80)
    print("\nDemonstrating standard compliance:")

    rfc_examples = [
        ("Standard equality operator (==)", "$[?@.age == 30]"),
        ("Standard logical AND (&&)", "$[?@.age >= 18 && @.age <= 65]"),
        ("Standard logical OR (||)", "$[?@.age < 18 || @.age > 65]"),
        ("Standard NOT operator (!)", "$[?!(@.active == true)]"),
    ]

    for idx, (description, jsonpath) in enumerate(rfc_examples, 1):
        print(f"\n{idx}. {description}")
        print(f"   JSONPath: {jsonpath}")
        print(f"   SQL:")
        try:
            sql = compiler.compile(jsonpath)
            for line in sql.split("\n"):
                print(f"      {line}")
        except Exception as e:
            print(f"      Error: {e}")

    print("\n" + "=" * 80)
    print("Advanced Features: Parentheses, Nested Paths, and Nested Wildcards")
    print("=" * 80)
    print("\nDemonstrating advanced query capabilities:")

    advanced_examples = [
        (
            "Parentheses for complex logic",
            "$[?(@.age >= 25 && @.age <= 60) || @.name == 'Admin']",
            "Group conditions: (age between 25-60) OR name is Admin"
        ),
        (
            "Nested path: access related field",
            "$[?@.orders.total > 100]",
            "Filter users by their orders' total (uses JOIN)"
        ),
        (
            "Nested path: multiple related conditions",
            "$[?@.orders.total > 100 && @.orders.status == 'completed']",
            "Filter by multiple fields in related table"
        ),
        (
            "Nested path + direct field",
            "$[?@.age > 18 && @.orders.total > 50]",
            "Combine direct field with nested path"
        ),
        (
            "Nested wildcard: filter parent by child",
            "$.orders[*][?@.order_items[*][?@.price > 100]]",
            "Find orders that have at least one item priced > 100 (uses EXISTS)"
        ),
        (
            "Nested wildcard at root level",
            "$[?@.orders[*][?@.total > 500]]",
            "Find users who have at least one order with total > 500"
        ),
        (
            "Combined: parentheses + nested wildcards",
            "$[?(@.age > 18 && @.active == true) && @.orders[*][?@.total > 100]]",
            "Active adults with at least one order > 100"
        ),
    ]

    for idx, (description, jsonpath, explanation) in enumerate(advanced_examples, 1):
        print(f"\n{idx}. {description}")
        print(f"   {explanation}")
        print(f"   JSONPath: {jsonpath}")
        print(f"   SQL:")
        try:
            sql = compiler.compile(jsonpath)
            for line in sql.split("\n"):
                print(f"      {line}")
        except Exception as e:
            print(f"      Error: {e}")

    print("\n" + "=" * 80)
    print("\nKey Features Demonstrated:")
    print("  1. Parentheses: Group logical conditions for correct precedence")
    print("  2. Nested Paths: Access related fields via @.relationship.field (uses JOIN)")
    print("  3. Nested Wildcards: Filter by child collection via @.rel[*][?...] (uses EXISTS)")
    print("  4. EXISTS Subqueries: Efficient 'at least one' semantics for wildcards")
    print("  5. Arbitrary Depth: Supports unlimited nesting levels")
    print("=" * 80)


if __name__ == "__main__":
    main()
