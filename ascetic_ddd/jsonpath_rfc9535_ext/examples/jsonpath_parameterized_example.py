"""Example usage of JSONPath RFC 9535 parser with placeholders."""
from ascetic_ddd.jsonpath_rfc9535_ext.domain.jsonpath_parameterized_parser import parse

print("=== JSONPath RFC 9535 Parser with Placeholders ===\n")

# Example data
users = [
    {"name": "Alice", "age": 30, "active": True},
    {"name": "Bob", "age": 25, "active": False},
    {"name": "Charlie", "age": 35, "active": True},
]

# Example 1: Positional placeholders (%d)
print("Example 1: Positional placeholder (%d)")
print("  Template: $[?@.age > %d]")
print("  Data: 3 users")

expr = parse("$[?@.age > %d]")

print("\n  Execute with params=(27,)")
results = expr.find(users, (27,))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

print("\n  Execute with params=(30,)")
results = expr.find(users, (30,))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 2: Named placeholders (%(name)s)
print("\n\nExample 2: Named placeholder (%(name)s)")
print("  Template: $[?@.name == %(name)s]")

expr2 = parse("$[?@.name == %(name)s]")

print("\n  Execute with params={'name': 'Alice'}")
results = expr2.find(users, {"name": "Alice"})
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 3: Reusing expression with different values
print("\n\nExample 3: Reusing expression with different values")
print("  Template: $[?@.age > %(min_age)d]")

expr3 = parse("$[?@.age > %(min_age)d]")

for min_age in [26, 30, 40]:
    results = expr3.find(users, {"min_age": min_age})
    print(f"  Min age {min_age}: {len(results)} users")

# Example 4: find_one() method
print("\n\nExample 4: find_one() method")
expr4 = parse("$[?@.active == %(active)s]")
first_active = expr4.find_one(users, {"active": True})
if first_active:
    print(f"  First active user: {first_active['name']}")

# Example 5: String placeholders
print("\n\nExample 5: String placeholders (%s)")
print("  Template: $[?@.name == %s]")

expr5 = parse("$[?@.name == %s]")
results = expr5.find(users, ("Alice",))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 6: AND operator (&&)
print("\n\nExample 6: AND operator (&&)")
print("  Template: $[?@.age > %d && @.active == %s]")

expr6 = parse("$[?@.age > %d && @.active == %s]")
results = expr6.find(users, (26, True))
print(f"  Results (age > 26 AND active): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}, active={user['active']}")

# Example 7: OR operator (||)
print("\n\nExample 7: OR operator (||)")
print("  Template: $[?@.age < %d || @.age > %d]")

expr7 = parse("$[?@.age < %d || @.age > %d]")
results = expr7.find(users, (28, 32))
print(f"  Results (age < 28 OR age > 32): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 8: NOT operator (!)
print("\n\nExample 8: NOT operator (!)")
print("  Template: $[?!(@.active == %s)]")

expr8 = parse("$[?!(@.active == %s)]")
results = expr8.find(users, (True,))
print(f"  Results (NOT active): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, active={user['active']}")

# Example 9: Nested data with filter
print("\n\nExample 9: Nested data with filter")
data_with_groups = {
    "users": [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
}
print("  Template: $.users[?@.age > %d]")

expr9 = parse("$.users[?@.age > %d]")
results = expr9.find(data_with_groups, (27,))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 10: Multiple placeholders
print("\n\nExample 10: Multiple placeholders")
print("  Template: $[?@.age >= %(min_age)d && @.age <= %(max_age)d]")

expr10 = parse("$[?@.age >= %(min_age)d && @.age <= %(max_age)d]")
results = expr10.find(users, {"min_age": 26, "max_age": 32})
print(f"  Results (26 <= age <= 32): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 11: finditer() method
print("\n\nExample 11: finditer() method")
expr11 = parse("$[?@.age > %d]")
print("  Iterating over results:")
for user in expr11.finditer(users, (27,)):
    print(f"    - {user['name']}, age {user['age']}")

print("\n=== All examples completed successfully! ===")
