"""Example usage of JSONPath Specification Parser using jsonpath-rfc9535 (RFC 9535 compliant)."""
from typing import Any

from ascetic_ddd.specification.domain.jsonpath.jsonpath_rfc9535_parser import parse


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
    """Demonstrate RFC 9535 JSONPath Specification Parser usage."""
    print("=== RFC 9535 JSONPath Specification Parser Examples ===\n")
    print("Note: This implementation is fully compliant with RFC 9535 standard")
    print("  - Uses == for equality (not single =)")
    print("  - Uses && for logical AND")
    print("  - Uses || for logical OR")
    print("  - Uses ! for logical NOT\n")

    # Example 1: Basic comparison with positional placeholder
    print("Example 1: Age filter with positional placeholder")
    print("  Specification: $[?@.age > %d]")
    spec = parse("$[?@.age > %d]")

    user1 = DictContext({"name": "Alice", "age": 30})
    user2 = DictContext({"name": "Bob", "age": 25})

    print(f"  User Alice (age 30) > 27: {spec.match(user1, (27,))}")  # True
    print(f"  User Bob (age 25) > 27: {spec.match(user2, (27,))}")  # False

    # Example 2: String equality with positional placeholder (RFC 9535 uses ==)
    print("\nExample 2: Name filter with string placeholder")
    print("  Specification: $[?@.name == %s]")
    print("  RFC 9535 standard: equality uses == (double equals)")
    spec2 = parse("$[?@.name == %s]")

    print(f"  User Alice name == 'Alice': {spec2.match(user1, ('Alice',))}")  # True
    print(f"  User Alice name == 'Bob': {spec2.match(user1, ('Bob',))}")  # False

    # Example 3: Named placeholders
    print("\nExample 3: Named placeholders")
    print("  Specification: $[?@.age >= %(min_age)d]")
    spec3 = parse("$[?@.age >= %(min_age)d]")

    print(f"  User Alice (age 30) >= 25: {spec3.match(user1, {'min_age': 25})}")  # True
    print(f"  User Bob (age 25) >= 30: {spec3.match(user2, {'min_age': 30})}")  # False

    # Example 4: Reusing specification with different parameters
    print("\nExample 4: Reusing specification")
    print("  Specification: $[?@.score >= %f]")
    spec4 = parse("$[?@.score >= %f]")

    student = DictContext({"name": "Emma", "score": 85.5})

    print(f"  Emma (score 85.5) >= 80.0: {spec4.match(student, (80.0,))}")  # True
    print(f"  Emma (score 85.5) >= 90.0: {spec4.match(student, (90.0,))}")  # False
    print(f"  Emma (score 85.5) >= 85.5: {spec4.match(student, (85.5,))}")  # True

    # Example 5: Different operators
    print("\nExample 5: Different comparison operators")

    product = DictContext({"name": "Widget", "price": 49.99, "stock": 100})

    # Less than
    spec_lt = parse("$[?@.price < %f]")
    print(f"  Price < 50.00: {spec_lt.match(product, (50.00,))}")  # True

    # Not equal (RFC 9535)
    spec_ne = parse("$[?@.stock != %d]")
    print(f"  Stock != 50: {spec_ne.match(product, (50,))}")  # True

    # Less than or equal
    spec_lte = parse("$[?@.stock <= %d]")
    print(f"  Stock <= 100: {spec_lte.match(product, (100,))}")  # True

    # Example 6: Logical AND operator (RFC 9535 uses &&)
    print("\nExample 6: Logical AND operator (&&)")
    print("  Specification: $[?@.age > %d && @.active == %s]")
    spec_and = parse("$[?@.age > %d && @.active == %s]")

    user_active = DictContext({"name": "Charlie", "age": 35, "active": True})
    user_inactive = DictContext({"name": "Dave", "age": 35, "active": False})

    print(f"  Charlie (age 35, active) > 30 && active: {spec_and.match(user_active, (30, True))}")  # True
    print(f"  Dave (age 35, inactive) > 30 && active: {spec_and.match(user_inactive, (30, True))}")  # False

    # Example 7: Logical OR operator (RFC 9535 uses ||)
    print("\nExample 7: Logical OR operator (||)")
    print("  Specification: $[?@.age < %d || @.age > %d]")
    spec_or = parse("$[?@.age < %d || @.age > %d]")

    user_young = DictContext({"name": "Emma", "age": 16})
    user_middle = DictContext({"name": "Frank", "age": 40})
    user_old = DictContext({"name": "Grace", "age": 70})

    print(f"  Emma (age 16) < 18 || > 65: {spec_or.match(user_young, (18, 65))}")  # True
    print(f"  Frank (age 40) < 18 || > 65: {spec_or.match(user_middle, (18, 65))}")  # False
    print(f"  Grace (age 70) < 18 || > 65: {spec_or.match(user_old, (18, 65))}")  # True

    # Example 8: Logical NOT operator (RFC 9535 uses !)
    print("\nExample 8: Logical NOT operator (!)")
    print("  Specification: $[?!(@.active == %s)]")
    spec_not = parse("$[?!(@.active == %s)]")

    print(f"  User active !(active == true): {spec_not.match(user_active, (True,))}")  # False
    print(f"  User inactive !(active == true): {spec_not.match(user_inactive, (True,))}")  # True

    # Example 9: Complex expression with multiple operators
    print("\nExample 9: Complex expression")
    print("  Specification: $[?(@.age >= %d && @.age <= %d) && @.status == %s]")
    spec_complex = parse("$[?(@.age >= %d && @.age <= %d) && @.status == %s]")

    user_valid = DictContext({"name": "Helen", "age": 30, "status": "active"})
    user_invalid_age = DictContext({"name": "Ivan", "age": 20, "status": "active"})
    user_invalid_status = DictContext({"name": "Jane", "age": 30, "status": "inactive"})

    print(f"  Helen (age 30, active) in range [25-35] && active: {spec_complex.match(user_valid, (25, 35, 'active'))}")  # True
    print(f"  Ivan (age 20, active) in range [25-35] && active: {spec_complex.match(user_invalid_age, (25, 35, 'active'))}")  # False
    print(f"  Jane (age 30, inactive) in range [25-35] && active: {spec_complex.match(user_invalid_status, (25, 35, 'active'))}")  # False

    # Example 10: Boolean values
    print("\nExample 10: Boolean values in comparisons")
    print("  Specification: $[?@.verified == %s]")
    spec_bool = parse("$[?@.verified == %s]")

    user_verified = DictContext({"name": "Kate", "verified": True})
    user_unverified = DictContext({"name": "Leo", "verified": False})

    print(f"  Kate (verified: true) == true: {spec_bool.match(user_verified, (True,))}")  # True
    print(f"  Leo (verified: false) == true: {spec_bool.match(user_unverified, (True,))}")  # False

    # Example 11: Multiple named placeholders
    print("\nExample 11: Multiple named placeholders")
    print("  Specification: $[?@.age >= %(min_age)d && @.age <= %(max_age)d]")
    spec_named = parse("$[?@.age >= %(min_age)d && @.age <= %(max_age)d]")

    user_mid = DictContext({"name": "Mike", "age": 30})

    params_valid = {"min_age": 25, "max_age": 35}
    params_invalid = {"min_age": 35, "max_age": 40}

    print(f"  Mike (age 30) in range [25-35]: {spec_named.match(user_mid, params_valid)}")  # True
    print(f"  Mike (age 30) in range [35-40]: {spec_named.match(user_mid, params_invalid)}")  # False

    # Example 12: RFC 9535 compliance demonstration
    print("\nExample 12: RFC 9535 Standard Compliance")
    print("  This implementation is fully compliant with RFC 9535:")
    print("  ✓ Uses == for equality (not single =)")
    print("  ✓ Uses && for logical AND (not 'and')")
    print("  ✓ Uses || for logical OR (not 'or')")
    print("  ✓ Uses ! for logical NOT (not 'not')")
    print("  ✓ Supports @ for current node reference")
    print("  ✓ Supports all standard comparison operators: ==, !=, <, >, <=, >=")

    spec_rfc = parse("$[?@.age >= %d && (@.status == %s || @.role == %s)]")
    user_rfc = DictContext({"name": "Nancy", "age": 28, "status": "active", "role": "admin"})

    print(f"\n  Complex RFC 9535 expression:")
    print(f"  Nancy (age 28, status: active, role: admin)")
    print(f"  age >= 25 && (status == 'active' || role == 'admin'): {spec_rfc.match(user_rfc, (25, 'active', 'admin'))}")  # True

    print("\n=== End of Examples ===")


if __name__ == "__main__":
    main()
