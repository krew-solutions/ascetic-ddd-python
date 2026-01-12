"""Example usage of jsonpath parser with placeholders."""
from ascetic_ddd.jsonpath2_ext.domain.jsonpath2_parameterized_parser import parse

print("=== JSONPath Parser with Placeholders ===\n")

# Example data
users = [
    {"name": "Alice", "age": 30, "active": True},
    {"name": "Bob", "age": 25, "active": False},
    {"name": "Charlie", "age": 35, "active": True},
]

# Example 1: Positional placeholders (%d)
print("Example 1: Positional placeholder (%d)")
print("  Template: $[*][?(@.age > %d)]")
print("  Data: 3 users")

path = parse("$[*][?(@.age > %d)]")

print("\n  Execute with params=(27,)")
results = path.find(users, (27,))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

print("\n  Execute with params=(30,)")
results = path.find(users, (30,))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 2: Named placeholders (%(name)s)
print("\n\nExample 2: Named placeholder (%(name)s)")
print("  Template: $[*][?(@.name = %(name)s)]")

path2 = parse("$[*][?(@.name = %(name)s)]")

print("\n  Execute with params={'name': 'Alice'}")
results = path2.find(users, {"name": "Alice"})
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# Example 3: Reusing path with different values
print("\n\nExample 3: Reusing path with different values")
print("  Template: $[*][?(@.age > %(min_age)d)]")

path3 = parse("$[*][?(@.age > %(min_age)d)]")

for min_age in [26, 30, 40]:
    results = path3.find(users, {"min_age": min_age})
    print(f"  Min age {min_age}: {len(results)} users")

# Example 4: find_one() method
print("\n\nExample 4: find_one() method")
path4 = parse("$[*][?(@.active = %(active)s)]")
first_active = path4.find_one(users, {"active": True})
if first_active:
    print(f"  First active user: {first_active['name']}")

# Example 5: Using match() directly
print("\n\nExample 5: Using match() for iteration")
path5 = parse("$[*][?(@.age > %d)]")
print("  Iterating over matches:")
for match in path5.match(users, (26,)):
    user = match.current_value
    print(f"    - {user['name']}")

# Example 6: Double equals (==) support
print("\n\nExample 6: Double equals (==) support")
print("  Note: Both = and == are supported (auto-normalized)")
print("  Template with ==: $[*][?(@.name == %s)]")

path6_double = parse("$[*][?(@.name == %s)]")
results = path6_double.find(users, ("Alice",))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

print("\n  Template with =: $[*][?(@.name = %s)]")
path6_single = parse("$[*][?(@.name = %s)]")
results = path6_single.find(users, ("Alice",))
print(f"  Results: {len(results)} users (identical behavior)")

# Example 7: RFC 9535 logical operators (&&, ||, !)
print("\n\nExample 7: RFC 9535 logical operators (&&, ||, !)")
print("  Note: RFC 9535 operators are auto-normalized to jsonpath2 format")
print("  && → and, || → or, ! → not")

# AND operator
print("\n  AND operator (&&):")
print("  Template: $[*][?(@.age > %d && @.active == %s)]")
path7_and = parse("$[*][?(@.age > %d && @.active == %s)]")
results = path7_and.find(users, (26, True))
print(f"  Results (age > 26 AND active): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}, active={user['active']}")

# OR operator
print("\n  OR operator (||):")
print("  Template: $[*][?(@.age < %d || @.age > %d)]")
path7_or = parse("$[*][?(@.age < %d || @.age > %d)]")
results = path7_or.find(users, (27, 32))
print(f"  Results (age < 27 OR age > 32): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}")

# NOT operator
print("\n  NOT operator (!):")
print("  Template: $[*][?(!(@.active == %s))]")
path7_not = parse("$[*][?(!(@.active == %s))]")
results = path7_not.find(users, (True,))
print(f"  Results (NOT active): {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}, active={user['active']}")

# Complex expression
print("\n  Complex expression:")
print("  Template: $[*][?(@.age >= %d && (@.name == %s || @.active == %s))]")
path7_complex = parse("$[*][?(@.age >= %d && (@.name == %s || @.active == %s))]")
results = path7_complex.find(users, (28, "Bob", True))
print(f"  Results: {len(results)} users")
for user in results:
    print(f"    - {user['name']}, age {user['age']}, active={user['active']}")

print("\n[OK] All examples completed successfully!")
print("\nKey features:")
print("  ✨ Supports both = and == for equality (auto-normalized)")
print("  ✨ Supports RFC 9535 logical operators: &&, ||, ! (auto-normalized)")
print("  • Positional placeholders: %s, %d, %f")
print("  • Named placeholders: %(name)s, %(age)d, %(price)f")
print("  • Reusable with different parameter values")
print("  • Full RFC 9535 compliance!")
