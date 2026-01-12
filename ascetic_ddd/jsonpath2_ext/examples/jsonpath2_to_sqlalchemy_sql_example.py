"""
Example usage of JSONPath to SQL compiler.

Demonstrates how to compile JSONPath expressions into SQL queries
for normalized relational databases.
"""
from sqlalchemy import Column, Integer, String, Float, MetaData, Table

from ..infrastructure.jsonpath2_to_sqlalchemy_sql import JSONPathToSQLCompiler, SchemaMetadata, RelationshipMetadata


def create_example_schema() -> SchemaMetadata:
    """Create example schema with users, orders, and products tables."""
    metadata = MetaData()

    # Define tables
    users_table = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer),
        Column("email", String),
    )

    orders_table = Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer),  # Foreign key to users
        Column("total", Float),
        Column("status", String),
    )

    order_items_table = Table(
        "order_items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("order_id", Integer),  # Foreign key to orders
        Column("product_id", Integer),  # Foreign key to products
        Column("quantity", Integer),
        Column("price", Float),
    )

    products_table = Table(
        "products",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("price", Float),
        Column("category", String),
    )

    # Define relationships
    # users -> orders (one-to-many)
    # orders -> order_items (one-to-many)
    # order_items -> product (many-to-one)
    relationships = {
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
        "order_items": {
            "product": RelationshipMetadata(
                target_table="products",
                foreign_key="product_id",
                target_primary_key="id",
                relationship_type="many-to-one",
            ),
        },
    }

    schema = SchemaMetadata(
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


def main():
    """Run examples."""
    schema = create_example_schema()
    compiler = JSONPathToSQLCompiler(schema)

    print("=" * 80)
    print("JSONPath to SQL Compiler Examples")
    print("=" * 80)

    # Example 1: Simple field access
    print("\n1. Simple field access: $.name")
    jsonpath1 = "$.name"
    query1 = compiler.compile(jsonpath1)
    print(f"   JSONPath: {jsonpath1}")
    print(f"   SQL: {query1}")

    # Example 2: Filter on age
    print("\n2. Filter on age: $[?(@.age > 18)]")
    jsonpath2 = "$[?(@.age > 18)]"
    try:
        query2 = compiler.compile(jsonpath2)
        print(f"   JSONPath: {jsonpath2}")
        print(f"   SQL: {query2}")
    except Exception as e:
        print(f"   Error: {e}")

    # Example 3: Navigate to related table
    print("\n3. Navigate to orders: $.orders")
    jsonpath3 = "$.orders"
    try:
        query3 = compiler.compile(jsonpath3)
        print(f"   JSONPath: {jsonpath3}")
        print(f"   SQL: {query3}")
    except Exception as e:
        print(f"   Error: {e}")

    # Example 4: Navigate and filter
    print("\n4. Navigate and filter: $.orders[?(@.total > 100)]")
    jsonpath4 = "$.orders[?(@.total > 100)]"
    try:
        query4 = compiler.compile(jsonpath4)
        print(f"   JSONPath: {jsonpath4}")
        print(f"   SQL: {query4}")
    except Exception as e:
        print(f"   Error: {e}")

    # Example 5: Deep navigation
    print("\n5. Deep navigation: $.orders.items.product")
    jsonpath5 = "$.orders.items.product"
    try:
        query5 = compiler.compile(jsonpath5)
        print(f"   JSONPath: {jsonpath5}")
        print(f"   SQL: {query5}")
    except Exception as e:
        print(f"   Error: {e}")

    # Example 6: Wildcard
    print("\n6. Wildcard: $[*]")
    jsonpath6 = "$[*]"
    try:
        query6 = compiler.compile(jsonpath6)
        print(f"   JSONPath: {jsonpath6}")
        print(f"   SQL: {query6}")
    except Exception as e:
        print(f"   Error: {e}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
