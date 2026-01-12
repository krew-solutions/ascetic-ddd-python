"""Example usage of Native JSONPath Specification Parser (no external dependencies)."""
from typing import Any

from ascetic_ddd.specification.domain.jsonpath.jsonpath_native_parser import parse


class DictContext:
    """Simple dictionary-based context implementation."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found")
        return self._data[key]


def main():
    """Demonstrate Native JSONPath Specification Parser usage."""
    print("=== Native JSONPath Specification Parser Examples ===")
    print("(Zero external dependencies - pure Python implementation)\n")

    # Example 1: Basic comparison with positional placeholder
    print("Example 1: Age filter with positional placeholder")
    print("  Specification: $[?(@.age > %d)]")
    spec = parse("$[?(@.age > %d)]")

    user1 = DictContext({"name": "Alice", "age": 30})
    user2 = DictContext({"name": "Bob", "age": 25})

    print(f"  User Alice (age 30) > 27: {spec.match(user1, (27,))}")  # True
    print(f"  User Bob (age 25) > 27: {spec.match(user2, (27,))}")  # False

    # Example 2: String equality with positional placeholder
    print("\nExample 2: Name filter with string placeholder")
    print("  Specification: $[?(@.name = %s)]")
    spec2 = parse("$[?(@.name = %s)]")

    print(f"  User Alice name = 'Alice': {spec2.match(user1, ('Alice',))}")  # True
    print(f"  User Alice name = 'Bob': {spec2.match(user1, ('Bob',))}")  # False

    # Example 3: Named placeholders
    print("\nExample 3: Named placeholders")
    print("  Specification: $[?(@.age >= %(min_age)d)]")
    spec3 = parse("$[?(@.age >= %(min_age)d)]")

    print(f"  User Alice (age 30) >= 25: {spec3.match(user1, {'min_age': 25})}")  # True
    print(f"  User Bob (age 25) >= 30: {spec3.match(user2, {'min_age': 30})}")  # False

    # Example 4: Multiple conditions with AND
    print("\nExample 4: Multiple conditions with AND operator")
    print("  Specification: $[?(@.age > %(min_age)d & @.active = %(active)s)]")
    spec4 = parse("$[?(@.age > %(min_age)d & @.active = %(active)s)]")

    active_user = DictContext({"name": "Charlie", "age": 35, "active": True})
    inactive_user = DictContext({"name": "Dave", "age": 40, "active": False})

    params = {"min_age": 30, "active": True}
    print(f"  Charlie (age 35, active=True): {spec4.match(active_user, params)}")  # True
    print(f"  Dave (age 40, active=False): {spec4.match(inactive_user, params)}")  # False

    # Example 5: Reusing specification with different parameters
    print("\nExample 5: Reusing specification")
    print("  Specification: $[?(@.score >= %f)]")
    spec5 = parse("$[?(@.score >= %f)]")

    student = DictContext({"name": "Emma", "score": 85.5})

    print(f"  Emma (score 85.5) >= 80.0: {spec5.match(student, (80.0,))}")  # True
    print(f"  Emma (score 85.5) >= 90.0: {spec5.match(student, (90.0,))}")  # False
    print(f"  Emma (score 85.5) >= 85.5: {spec5.match(student, (85.5,))}")  # True

    # Example 6: Different operators
    print("\nExample 6: Different comparison operators")

    product = DictContext({"name": "Widget", "price": 49.99, "stock": 100})

    # Less than
    spec_lt = parse("$[?(@.price < %f)]")
    print(f"  Price < 50.00: {spec_lt.match(product, (50.00,))}")  # True

    # Not equal
    spec_ne = parse("$[?(@.stock != %d)]")
    print(f"  Stock != 50: {spec_ne.match(product, (50,))}")  # True

    # Less than or equal
    spec_lte = parse("$[?(@.stock <= %d)]")
    print(f"  Stock <= 100: {spec_lte.match(product, (100,))}")  # True

    # Example 7: Collection wildcard
    print("\nExample 7: Collection with wildcard")
    print("  Specification: $.items[*][?(@.score > %d)]")

    from ..evaluate_visitor import CollectionContext

    spec7 = parse("$.items[*][?(@.score > %d)]")

    # Create collection of items
    item1 = DictContext({"name": "Alice", "score": 90})
    item2 = DictContext({"name": "Bob", "score": 75})
    item3 = DictContext({"name": "Charlie", "score": 85})

    collection = CollectionContext([item1, item2, item3])
    root_ctx = DictContext({"items": collection})

    print(f"  Items with score > 80: {spec7.match(root_ctx, (80,))}")  # True (Alice, Charlie)
    print(f"  Items with score > 95: {spec7.match(root_ctx, (95,))}")  # False

    # Example 8: Collection with string filtering
    print("\nExample 8: Collection string filtering")
    print("  Specification: $.users[*][?(@.role = %s)]")

    spec8 = parse("$.users[*][?(@.role = %s)]")

    user1 = DictContext({"name": "Alice", "role": "admin"})
    user2 = DictContext({"name": "Bob", "role": "user"})
    user3 = DictContext({"name": "Charlie", "role": "admin"})

    users_collection = CollectionContext([user1, user2, user3])
    users_root = DictContext({"users": users_collection})

    print(f"  Has admin users: {spec8.match(users_root, ('admin',))}")  # True
    print(f"  Has guest users: {spec8.match(users_root, ('guest',))}")  # False

    # Example 9: Boolean values
    print("\nExample 9: Boolean value comparisons")
    print("  Specification: $[?(@.active = %s)]")

    spec9 = parse("$[?(@.active = %s)]")

    active_usr = DictContext({"name": "Alice", "active": True})
    inactive_usr = DictContext({"name": "Bob", "active": False})

    print(f"  Active user matches True: {spec9.match(active_usr, (True,))}")  # True
    print(f"  Inactive user matches False: {spec9.match(inactive_usr, (False,))}")  # True

    # Example 10: Demonstrating the lexer
    print("\nExample 10: Behind the scenes - Lexer tokenization")
    print("  Expression: $[?(@.age > 25)]")

    from .jsonpath_native_parser import Lexer

    lexer = Lexer("$[?(@.age > 25)]")
    tokens = lexer.tokenize()

    print("  Tokens generated:")
    for token in tokens:
        print(f"    {token.type:12s} -> {token.value!r}")

    # Example 11: Performance - parse once, use many times
    print("\nExample 11: Performance optimization - parse once, use many times")
    print("  Specification: $[?(@.age >= %d)]")

    spec11 = parse("$[?(@.age >= %d)]")

    users = [
        DictContext({"name": "Alice", "age": 30}),
        DictContext({"name": "Bob", "age": 25}),
        DictContext({"name": "Charlie", "age": 35}),
        DictContext({"name": "Dave", "age": 20}),
    ]

    print("  Testing multiple users with min_age=28:")
    for user in users:
        name = user.get("name")
        age = user.get("age")
        matches = spec11.match(user, (28,))
        status = "✓" if matches else "✗"
        print(f"    {status} {name} (age {age})")

    print("\n=== All examples completed successfully! ===")
    print("\nKey advantages of Native parser:")
    print("  • Zero external dependencies")
    print("  • Full control over parsing logic")
    print("  • Lightweight (~500 lines of code)")
    print("  • Easy to customize and extend")
    print("  • Supports all comparison operators")
    print("  • Full AND/OR operator support")
    print("  • Compatible API with other parsers")


if __name__ == "__main__":
    main()
