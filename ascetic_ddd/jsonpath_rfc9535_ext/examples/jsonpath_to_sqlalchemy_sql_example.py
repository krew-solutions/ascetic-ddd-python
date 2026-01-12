"""
Example usage of JSONPath RFC 9535 to SQLAlchemy SQL compiler.

Demonstrates how to compile RFC 9535 compliant JSONPath expressions
into SQLAlchemy Select queries.
"""
try:
    from sqlalchemy import MetaData, Table, Column, Integer, String, Numeric, Boolean, ForeignKey
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    print("SQLAlchemy is not installed. Install with: pip install sqlalchemy")

from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_sqlalchemy_sql import (
    JSONPathToSQLAlchemyCompiler, SchemaMetadata, RelationshipMetadata, RelationType
)


def create_example_schema() -> 'SchemaMetadata':
    """Create example schema with SQLAlchemy Tables."""
    if not HAS_SQLALCHEMY:
        raise ImportError("SQLAlchemy is required for this example")

    metadata = MetaData()

    # Define tables
    users = Table(
        'users',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), nullable=False),
        Column('age', Integer),
        Column('email', String(255)),
        Column('active', Boolean, nullable=False),
    )

    orders = Table(
        'orders',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
        Column('total', Numeric(10, 2), nullable=False),
        Column('status', String(50), nullable=False),
    )

    products = Table(
        'products',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), nullable=False),
        Column('price', Numeric(10, 2), nullable=False),
        Column('category', String(50)),
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

    schema = SchemaMetadata(
        tables={
            'users': users,
            'orders': orders,
            'products': products,
        },
        relationships=relationships,
        root_table='users',
    )

    return schema


def main():
    """Run examples."""
    if not HAS_SQLALCHEMY:
        print("SQLAlchemy is not installed. Install with: pip install sqlalchemy")
        return

    schema = create_example_schema()
    compiler = JSONPathToSQLAlchemyCompiler(schema)

    print("=" * 80)
    print("JSONPath RFC 9535 to SQLAlchemy SQL Compiler Examples")
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
    ]

    for idx, (description, jsonpath) in enumerate(examples, 1):
        print(f"\n{idx}. {description}")
        print(f"   JSONPath: {jsonpath}")
        print(f"   SQLAlchemy Query:")
        try:
            query = compiler.compile(jsonpath)
            # Format query for display
            print(f"      {query}")
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
        print(f"   SQLAlchemy Query:")
        try:
            query = compiler.compile(jsonpath)
            print(f"      {query}")
        except Exception as e:
            print(f"      Error: {e}")

    print("\n" + "=" * 80)
    print("\nNote: SQLAlchemy queries can be executed like:")
    print("  with engine.connect() as conn:")
    print("      result = conn.execute(query)")
    print("=" * 80)


if __name__ == "__main__":
    main()
