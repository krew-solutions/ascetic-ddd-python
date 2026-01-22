"""Example usage of JSONPath Specification Parser using jsonpath2."""
from typing import Any

from ascetic_ddd.specification.domain.jsonpath.jsonpath2_parser import parse


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
    """Demonstrate JSONPath2 Specification Parser usage."""
    print("=== JSONPath2 Specification Parser Examples ===\n")

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
    print("  Specification: $[?(@.name == %s)]")
    print("  Note: jsonpath2 now supports both = and == (auto-normalized)")
    spec2 = parse("$[?(@.name == %s)]")

    print(f"  User Alice name == 'Alice': {spec2.match(user1, ('Alice',))}")  # True
    print(f"  User Alice name == 'Bob': {spec2.match(user1, ('Bob',))}")  # False

    # Example 3: Named placeholders
    print("\nExample 3: Named placeholders")
    print("  Specification: $[?(@.age >= %(min_age)d)]")
    spec3 = parse("$[?(@.age >= %(min_age)d)]")

    print(f"  User Alice (age 30) >= 25: {spec3.match(user1, {'min_age': 25})}")  # True
    print(f"  User Bob (age 25) >= 30: {spec3.match(user2, {'min_age': 30})}")  # False

    # Example 4: Reusing specification with different parameters
    print("\nExample 4: Reusing specification")
    print("  Specification: $[?(@.score >= %f)]")
    spec4 = parse("$[?(@.score >= %f)]")

    student = DictContext({"name": "Emma", "score": 85.5})

    print(f"  Emma (score 85.5) >= 80.0: {spec4.match(student, (80.0,))}")  # True
    print(f"  Emma (score 85.5) >= 90.0: {spec4.match(student, (90.0,))}")  # False
    print(f"  Emma (score 85.5) >= 85.5: {spec4.match(student, (85.5,))}")  # True

    # Example 5: Different operators
    print("\nExample 5: Different comparison operators")

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

    # Example 6: Collection wildcard
    print("\nExample 6: Collection with wildcard")
    print("  Specification: $.items[*][?(@.score > %d)]")

    from ..evaluate_visitor import CollectionContext

    spec6 = parse("$.items[*][?(@.score > %d)]")

    # Create collection of items
    item1 = DictContext({"name": "Alice", "score": 90})
    item2 = DictContext({"name": "Bob", "score": 75})
    item3 = DictContext({"name": "Charlie", "score": 85})

    collection = CollectionContext([item1, item2, item3])
    root_ctx = DictContext({"items": collection})

    print(f"  Items with score > 80: {spec6.match(root_ctx, (80,))}")  # True (Alice, Charlie)
    print(f"  Items with score > 95: {spec6.match(root_ctx, (95,))}")  # False

    # Example 7: Collection with string filtering
    print("\nExample 7: Collection string filtering")
    print("  Specification: $.users[*][?(@.role = %s)]")

    spec7 = parse("$.users[*][?(@.role = %s)]")

    user1 = DictContext({"name": "Alice", "role": "admin"})
    user2 = DictContext({"name": "Bob", "role": "user"})
    user3 = DictContext({"name": "Charlie", "role": "admin"})

    users_collection = CollectionContext([user1, user2, user3])
    users_root = DictContext({"users": users_collection})

    print(f"  Has admin users: {spec7.match(users_root, ('admin',))}")  # True
    print(f"  Has guest users: {spec7.match(users_root, ('guest',))}")  # False

    # Example 8: Boolean values
    print("\nExample 8: Boolean value comparisons")
    print("  Specification: $[?(@.active = %s)]")

    spec8 = parse("$[?(@.active = %s)]")

    active_user = DictContext({"name": "Alice", "active": True})
    inactive_user = DictContext({"name": "Bob", "active": False})

    print(f"  Active user matches True: {spec8.match(active_user, (True,))}")  # True
    print(f"  Inactive user matches False: {spec8.match(inactive_user, (False,))}")  # True

    # Example 9: RFC 9535 logical operators (&&, ||, !)
    print("\nExample 9: RFC 9535 logical operators")
    print("  Note: RFC 9535 operators are auto-normalized")
    print("  && → and, || → or, ! → not")

    # AND operator
    print("\n  AND operator (&&):")
    print("  Specification: $[?(@.age >= %(min_age)d && @.active == %(active)s)]")
    spec9_and = parse("$[?(@.age >= %(min_age)d && @.active == %(active)s)]")

    user_active_30 = DictContext({"name": "Alice", "age": 30, "active": True})
    user_inactive_35 = DictContext({"name": "Bob", "age": 35, "active": False})

    params_and = {"min_age": 25, "active": True}
    print(f"  Alice (30, active) matches: {spec9_and.match(user_active_30, params_and)}")  # True
    print(f"  Bob (35, inactive) matches: {spec9_and.match(user_inactive_35, params_and)}")  # False

    # OR operator
    print("\n  OR operator (||):")
    print("  Specification: $[?(@.age < %d || @.age > %d)]")
    spec9_or = parse("$[?(@.age < %d || @.age > %d)]")

    user_25 = DictContext({"age": 25})
    user_30 = DictContext({"age": 30})
    user_40 = DictContext({"age": 40})

    print(f"  Age 25 (< 27 or > 35): {spec9_or.match(user_25, (27, 35))}")  # True
    print(f"  Age 30 (< 27 or > 35): {spec9_or.match(user_30, (27, 35))}")  # False
    print(f"  Age 40 (< 27 or > 35): {spec9_or.match(user_40, (27, 35))}")  # True

    # NOT operator
    print("\n  NOT operator (!):")
    print("  Specification: $[?(!(@.deleted == %s))]")
    spec9_not = parse("$[?(!(@.deleted == %s))]")

    user_not_deleted = DictContext({"name": "Alice", "deleted": False})
    user_deleted = DictContext({"name": "Bob", "deleted": True})

    print(f"  Alice (not deleted) matches: {spec9_not.match(user_not_deleted, (True,))}")  # True
    print(f"  Bob (deleted) matches: {spec9_not.match(user_deleted, (True,))}")  # False

    # Complex expression
    print("\n  Complex expression:")
    print("  Specification: $[?(@.age >= %d && (@.role == %s || @.premium == %s))]")
    spec9_complex = parse("$[?(@.age >= %d && (@.role == %s || @.premium == %s))]")

    user_admin_30 = DictContext({"age": 30, "role": "admin", "premium": False})
    user_premium_25 = DictContext({"age": 25, "role": "user", "premium": True})
    user_basic_35 = DictContext({"age": 35, "role": "user", "premium": False})

    print(f"  Admin 30 (age>=25, admin or premium): {spec9_complex.match(user_admin_30, (25, 'admin', True))}")  # True
    print(f"  Premium 25 (age>=25, admin or premium): {spec9_complex.match(user_premium_25, (25, 'admin', True))}")  # True
    print(f"  Basic 35 (age>=25, admin or premium): {spec9_complex.match(user_basic_35, (25, 'admin', True))}")  # False

    print("\n=== All examples completed successfully! ===")
    print("\nKey features of jsonpath2:")
    print("  ✨ Supports both = and == for equality (auto-normalized)")
    print("  ✨ Supports RFC 9535 logical operators: &&, ||, ! (auto-normalized)")
    print("  • Full RFC 9535 JSONPath standard compliance")
    print("  • Strict syntax validation with detailed errors")
    print("  • Full compatibility with Native parsers")
    print("  • Fast parsing based on ANTLR")


if __name__ == "__main__":
    main()
