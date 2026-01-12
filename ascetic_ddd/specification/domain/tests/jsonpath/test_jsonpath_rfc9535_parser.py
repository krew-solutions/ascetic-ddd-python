"""Unit tests for JSONPath parser using jsonpath-rfc9535 library (RFC 9535 compliant)."""
import unittest
from typing import Any

from ...jsonpath.jsonpath_rfc9535_parser import parse


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found")
        return self._data[key]


class CollectionContext:
    """Collection context for wildcard testing."""

    def __init__(self, items: list[Any]):
        self._items = items

    def get(self, slice_: str) -> Any:
        """Get collection slice - returns items for wildcard."""
        if slice_ == "*":
            return self._items
        raise ValueError(f'Unsupported slice type "{slice_}"')


class NestedDictContext:
    """Nested dictionary-based context for testing nested paths."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key, supporting nested dict access."""
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found")

        value = self._data[key]

        # If value is a dict, wrap it in NestedDictContext
        if isinstance(value, dict):
            return NestedDictContext(value)

        return value


class TestJsonPathRFC9535Parser(unittest.TestCase):
    """Test JSONPath parser using jsonpath-rfc9535 (RFC 9535 compliant)."""

    def test_simple_comparison_greater_than(self):
        """Test simple greater-than comparison."""
        spec = parse("$[?@.age > %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (25,)))
        self.assertFalse(spec.match(user, (35,)))

    def test_simple_comparison_less_than(self):
        """Test simple less-than comparison."""
        spec = parse("$[?@.age < %d]")
        user = DictContext({"age": 25})

        self.assertTrue(spec.match(user, (30,)))
        self.assertFalse(spec.match(user, (20,)))

    def test_simple_comparison_equal(self):
        """Test simple equality comparison (RFC 9535 uses ==)."""
        spec = parse("$[?@.name == %s]")
        user = DictContext({"name": "Alice"})

        self.assertTrue(spec.match(user, ("Alice",)))
        self.assertFalse(spec.match(user, ("Bob",)))

    def test_simple_comparison_not_equal(self):
        """Test simple not-equal comparison."""
        spec = parse("$[?@.status != %s]")
        user = DictContext({"status": "active"})

        self.assertTrue(spec.match(user, ("inactive",)))
        self.assertFalse(spec.match(user, ("active",)))

    def test_greater_than_or_equal(self):
        """Test greater-than-or-equal comparison."""
        spec = parse("$[?@.age >= %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))  # Equal
        self.assertTrue(spec.match(user, (25,)))  # Greater
        self.assertFalse(spec.match(user, (35,)))  # Less

    def test_less_than_or_equal(self):
        """Test less-than-or-equal comparison."""
        spec = parse("$[?@.age <= %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))  # Equal
        self.assertTrue(spec.match(user, (35,)))  # Less
        self.assertFalse(spec.match(user, (25,)))  # Greater

    def test_named_placeholder(self):
        """Test named placeholder."""
        spec = parse("$[?@.age > %(min_age)d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, {"min_age": 25}))
        self.assertFalse(spec.match(user, {"min_age": 35}))

    def test_string_placeholder(self):
        """Test string placeholder."""
        spec = parse("$[?@.name == %(name)s]")
        user = DictContext({"name": "Alice"})

        self.assertTrue(spec.match(user, {"name": "Alice"}))
        self.assertFalse(spec.match(user, {"name": "Bob"}))

    def test_float_placeholder(self):
        """Test float placeholder."""
        spec = parse("$[?@.price > %f]")
        product = DictContext({"price": 99.99})

        self.assertTrue(spec.match(product, (50.0,)))
        self.assertFalse(spec.match(product, (100.0,)))

    def test_reuse_specification(self):
        """Test reusing specification with different parameters."""
        spec = parse("$[?@.age > %d]")

        user1 = DictContext({"age": 30})
        user2 = DictContext({"age": 20})
        user3 = DictContext({"age": 40})

        # Same spec, different parameters
        self.assertTrue(spec.match(user1, (25,)))
        self.assertFalse(spec.match(user2, (25,)))
        self.assertTrue(spec.match(user3, (25,)))

        # Different threshold
        self.assertFalse(spec.match(user1, (35,)))
        self.assertFalse(spec.match(user2, (35,)))
        self.assertTrue(spec.match(user3, (35,)))

    def test_logical_and_operator(self):
        """Test logical AND operator (RFC 9535 uses &&)."""
        spec = parse("$[?@.age > %d && @.active == %s]")
        user = DictContext({"age": 30, "active": True})

        self.assertTrue(spec.match(user, (25, True)))
        self.assertFalse(spec.match(user, (35, True)))
        self.assertFalse(spec.match(user, (25, False)))

    def test_logical_or_operator(self):
        """Test logical OR operator (RFC 9535 uses ||)."""
        spec = parse("$[?@.age < %d || @.age > %d]")
        user_young = DictContext({"age": 15})
        user_old = DictContext({"age": 70})
        user_middle = DictContext({"age": 40})

        self.assertTrue(spec.match(user_young, (18, 65)))
        self.assertTrue(spec.match(user_old, (18, 65)))
        self.assertFalse(spec.match(user_middle, (18, 65)))

    def test_logical_not_operator(self):
        """Test logical NOT operator (RFC 9535 uses !)."""
        spec = parse("$[?!(@.active == %s)]")
        user_active = DictContext({"active": True})
        user_inactive = DictContext({"active": False})

        self.assertTrue(spec.match(user_active, (False,)))
        self.assertFalse(spec.match(user_active, (True,)))
        self.assertFalse(spec.match(user_inactive, (False,)))
        self.assertTrue(spec.match(user_inactive, (True,)))

    def test_complex_logical_expression(self):
        """Test complex logical expression with multiple operators."""
        spec = parse("$[?(@.age >= %d && @.age <= %d) && @.status == %s]")
        user = DictContext({"age": 30, "status": "active"})

        self.assertTrue(spec.match(user, (25, 35, "active")))
        self.assertFalse(spec.match(user, (25, 35, "inactive")))
        self.assertFalse(spec.match(user, (35, 40, "active")))

    def test_boolean_values(self):
        """Test boolean values in comparisons."""
        spec = parse("$[?@.active == %s]")
        user_active = DictContext({"active": True})
        user_inactive = DictContext({"active": False})

        self.assertTrue(spec.match(user_active, (True,)))
        self.assertFalse(spec.match(user_active, (False,)))
        self.assertTrue(spec.match(user_inactive, (False,)))
        self.assertFalse(spec.match(user_inactive, (True,)))

    def test_multiple_positional_placeholders(self):
        """Test multiple positional placeholders."""
        spec = parse("$[?@.age >= %d && @.age <= %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (25, 35)))
        self.assertFalse(spec.match(user, (35, 40)))
        self.assertFalse(spec.match(user, (20, 25)))

    def test_multiple_named_placeholders(self):
        """Test multiple named placeholders."""
        spec = parse("$[?@.age >= %(min_age)d && @.age <= %(max_age)d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, {"min_age": 25, "max_age": 35}))
        self.assertFalse(spec.match(user, {"min_age": 35, "max_age": 40}))

    def test_mixed_types_placeholders(self):
        """Test mixed types in placeholders."""
        spec = parse("$[?@.name == %s && @.age > %d && @.active == %s]")
        user = DictContext({"name": "Alice", "age": 30, "active": True})

        self.assertTrue(spec.match(user, ("Alice", 25, True)))
        self.assertFalse(spec.match(user, ("Bob", 25, True)))
        self.assertFalse(spec.match(user, ("Alice", 35, True)))

    def test_string_with_special_characters(self):
        """Test string values with special characters."""
        spec = parse("$[?@.email == %s]")
        user = DictContext({"email": "alice@example.com"})

        self.assertTrue(spec.match(user, ("alice@example.com",)))
        self.assertFalse(spec.match(user, ("bob@example.com",)))

    def test_nested_field_access(self):
        """Test access to nested fields."""
        # Note: This test depends on DictContext supporting nested access
        # For simplicity, we test with flat structure
        spec = parse("$[?@.age > %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (25,)))

    def test_error_missing_positional_parameter(self):
        """Test error when positional parameter is missing."""
        spec = parse("$[?@.age > %d && @.active == %s]")
        user = DictContext({"age": 30, "active": True})

        with self.assertRaises(ValueError) as cm:
            spec.match(user, (25,))  # Missing second parameter

        self.assertIn("Missing positional parameter", str(cm.exception))

    def test_error_missing_named_parameter(self):
        """Test error when named parameter is missing."""
        spec = parse("$[?@.age > %(min_age)d && @.active == %(active)s]")
        user = DictContext({"age": 30, "active": True})

        with self.assertRaises(ValueError) as cm:
            spec.match(user, {"min_age": 25})  # Missing 'active' parameter

        self.assertIn("Missing named parameter", str(cm.exception))

    def test_error_missing_field_in_context(self):
        """Test error when field is missing in context."""
        spec = parse("$[?@.nonexistent == %s]")
        user = DictContext({"name": "Alice"})

        with self.assertRaises(KeyError):
            spec.match(user, ("value",))

    def test_rfc9535_standard_compliance(self):
        """Test RFC 9535 standard compliance features."""
        # RFC 9535 uses == for equality (not single =)
        spec_eq = parse("$[?@.age == %d]")
        user = DictContext({"age": 30})
        self.assertTrue(spec_eq.match(user, (30,)))

        # RFC 9535 uses && for AND
        spec_and = parse("$[?@.age > %d && @.active == %s]")
        user_active = DictContext({"age": 30, "active": True})
        self.assertTrue(spec_and.match(user_active, (25, True)))

        # RFC 9535 uses || for OR
        spec_or = parse("$[?@.age < %d || @.age > %d]")
        user_young = DictContext({"age": 15})
        self.assertTrue(spec_or.match(user_young, (18, 65)))

        # RFC 9535 uses ! for NOT
        spec_not = parse("$[?!(@.active == %s)]")
        user_inactive = DictContext({"active": False})
        self.assertTrue(spec_not.match(user_inactive, (True,)))


class TestJsonPathRFC9535Wildcards(unittest.TestCase):
    """Test wildcard functionality with RFC 9535."""

    def test_wildcard_collection_filter(self):
        """Test filtering items in a collection with wildcard."""
        spec = parse("$.items[*][?@.price > %f]")

        # Create test data
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        item2 = DictContext({"name": "Mouse", "price": 29.0})
        item3 = DictContext({"name": "Keyboard", "price": 79.0})

        items = CollectionContext([item1, item2, item3])
        store = DictContext({"items": items})

        # Should match: at least one item with price > 500
        self.assertTrue(spec.match(store, (500.0,)))

        # Should not match: no items with price > 1000
        self.assertFalse(spec.match(store, (1000.0,)))

    def test_nested_wildcard_simple(self):
        """Test nested wildcard with simple filter."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

        # Create nested data structure
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        item2 = DictContext({"name": "Mouse", "price": 29.0})
        items1 = CollectionContext([item1, item2])
        category1 = DictContext({"name": "Electronics", "items": items1})

        item3 = DictContext({"name": "Shirt", "price": 49.0})
        item4 = DictContext({"name": "Jeans", "price": 89.0})
        items2 = CollectionContext([item3, item4])
        category2 = DictContext({"name": "Clothing", "items": items2})

        categories = CollectionContext([category1, category2])
        store = DictContext({"categories": categories})

        # Should match: category1 has laptop with price > 500
        self.assertTrue(spec.match(store, (500.0,)))

        # Should not match: no items with price > 1000
        self.assertFalse(spec.match(store, (1000.0,)))

    def test_nested_wildcard_with_logical_operators(self):
        """Test nested wildcard with AND operator."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f && @.price < %f]]")

        # Create test data
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        item2 = DictContext({"name": "Mouse", "price": 29.0})
        items1 = CollectionContext([item1, item2])
        category1 = DictContext({"name": "Electronics", "items": items1})

        categories = CollectionContext([category1])
        store = DictContext({"categories": categories})

        # Should match: laptop price is between 500 and 1000
        self.assertTrue(spec.match(store, (500.0, 1000.0)))

        # Should not match: no items between 1000 and 2000
        self.assertFalse(spec.match(store, (1000.0, 2000.0)))

    def test_nested_wildcard_empty_collection(self):
        """Test nested wildcard with empty inner collection."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

        # Category with no items
        items = CollectionContext([])
        category = DictContext({"name": "Empty", "items": items})

        categories = CollectionContext([category])
        store = DictContext({"categories": categories})

        # Should not match: no items at all
        self.assertFalse(spec.match(store, (100.0,)))

    def test_nested_wildcard_multiple_matches(self):
        """Test nested wildcard where multiple categories match."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

        # Category 1 with expensive items
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        items1 = CollectionContext([item1])
        category1 = DictContext({"name": "Electronics", "items": items1})

        # Category 2 with expensive items
        item2 = DictContext({"name": "Designer Jeans", "price": 299.0})
        items2 = CollectionContext([item2])
        category2 = DictContext({"name": "Clothing", "items": items2})

        categories = CollectionContext([category1, category2])
        store = DictContext({"categories": categories})

        # Should match: both categories have items > 200
        self.assertTrue(spec.match(store, (200.0,)))


class TestJsonPathRFC9535NestedPaths(unittest.TestCase):
    """Test nested paths functionality with RFC 9535."""

    def test_nested_path_simple(self):
        """Test simple nested path: $[?@.profile.age > 25]."""
        spec = parse("$[?@.profile.age > %d]")

        # Test with age > 25
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "name": "Alice"
            }
        })

        self.assertTrue(spec.match(data, (25,)))

        # Test with age <= 25
        data = NestedDictContext({
            "profile": {
                "age": 20,
                "name": "Bob"
            }
        })

        self.assertFalse(spec.match(data, (25,)))

    def test_nested_path_deep(self):
        """Test deep nested path: $[?@.company.department.manager.level > 5]."""
        spec = parse("$[?@.company.department.manager.level > %d]")

        data = NestedDictContext({
            "company": {
                "department": {
                    "manager": {
                        "level": 7,
                        "name": "Alice"
                    }
                }
            }
        })

        self.assertTrue(spec.match(data, (5,)))

        # Test with level <= 5
        data = NestedDictContext({
            "company": {
                "department": {
                    "manager": {
                        "level": 3,
                        "name": "Bob"
                    }
                }
            }
        })

        self.assertFalse(spec.match(data, (5,)))

    def test_nested_path_with_and_operator(self):
        """Test nested path with AND operator."""
        spec = parse("$[?@.profile.age > %d && @.profile.active == %s]")

        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": True
            }
        })

        self.assertTrue(spec.match(data, (25, True)))

        # Test with active = False
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": False
            }
        })

        self.assertFalse(spec.match(data, (25, True)))

    def test_nested_path_with_or_operator(self):
        """Test nested path with OR operator."""
        spec = parse("$[?@.profile.age < %d || @.profile.age > %d]")

        data = NestedDictContext({
            "profile": {
                "age": 15
            }
        })

        self.assertTrue(spec.match(data, (18, 65)))

        data = NestedDictContext({
            "profile": {
                "age": 70
            }
        })

        self.assertTrue(spec.match(data, (18, 65)))

        data = NestedDictContext({
            "profile": {
                "age": 30
            }
        })

        self.assertFalse(spec.match(data, (18, 65)))

    def test_nested_path_equality(self):
        """Test nested path with equality comparison."""
        spec = parse("$[?@.profile.status == %s]")

        data = NestedDictContext({
            "profile": {
                "status": "active"
            }
        })

        self.assertTrue(spec.match(data, ("active",)))

        data = NestedDictContext({
            "profile": {
                "status": "inactive"
            }
        })

        self.assertFalse(spec.match(data, ("active",)))

    def test_nested_path_complex_expression(self):
        """Test nested path with complex expression."""
        spec = parse("$[?(@.profile.age >= %d && @.profile.age <= %d) && @.profile.active == %s]")

        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": True
            }
        })

        self.assertTrue(spec.match(data, (25, 35, True)))

        # Test with age out of range
        data = NestedDictContext({
            "profile": {
                "age": 70,
                "active": True
            }
        })

        self.assertFalse(spec.match(data, (25, 35, True)))

        # Test with active = False
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": False
            }
        })

        self.assertFalse(spec.match(data, (25, 35, True)))

    def test_nested_path_with_named_placeholder(self):
        """Test nested path with named placeholder."""
        spec = parse("$[?@.profile.age > %(min_age)d]")

        data = NestedDictContext({
            "profile": {
                "age": 30
            }
        })

        self.assertTrue(spec.match(data, {"min_age": 25}))
        self.assertFalse(spec.match(data, {"min_age": 35}))


if __name__ == "__main__":
    unittest.main()
