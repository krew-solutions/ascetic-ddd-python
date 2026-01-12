"""Unit tests for native JSONPath parser (without external dependencies)."""
import unittest
from typing import Any

from ...jsonpath.jsonpath_native_parser import Lexer
from ...jsonpath.jsonpath_native_parser import parse
from ...evaluate_visitor import CollectionContext


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found")
        return self._data[key]


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


class TestNativeParser(unittest.TestCase):
    """Test native JSONPath parser."""

    def test_simple_comparison_greater_than(self):
        """Test simple greater-than comparison."""
        spec = parse("$[?(@.age > %d)]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (25,)))
        self.assertFalse(spec.match(user, (35,)))

    def test_simple_comparison_less_than(self):
        """Test simple less-than comparison."""
        spec = parse("$[?(@.age < %d)]")
        user = DictContext({"age": 25})

        self.assertTrue(spec.match(user, (30,)))
        self.assertFalse(spec.match(user, (20,)))

    def test_simple_comparison_equal(self):
        """Test simple equality comparison (RFC 9535: ==)."""
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
        spec = parse("$[?(@.age >= %d)]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))  # Equal
        self.assertTrue(spec.match(user, (25,)))  # Greater
        self.assertFalse(spec.match(user, (35,)))  # Less

    def test_less_than_or_equal(self):
        """Test less-than-or-equal comparison."""
        spec = parse("$[?(@.age <= %d)]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))  # Equal
        self.assertTrue(spec.match(user, (35,)))  # Less
        self.assertFalse(spec.match(user, (25,)))  # Greater

    def test_named_placeholder(self):
        """Test named placeholder."""
        spec = parse("$[?(@.age > %(min_age)d)]")
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
        spec = parse("$[?(@.price > %f)]")
        product = DictContext({"price": 99.99})

        self.assertTrue(spec.match(product, (50.0,)))
        self.assertFalse(spec.match(product, (100.0,)))

    def test_reuse_specification(self):
        """Test reusing specification with different parameters."""
        spec = parse("$[?(@.age > %d)]")
        user = DictContext({"age": 30})

        # Multiple calls with different parameters
        self.assertTrue(spec.match(user, (25,)))
        self.assertFalse(spec.match(user, (35,)))
        self.assertTrue(spec.match(user, (20,)))

    def test_wildcard_collection(self):
        """Test wildcard collection filtering."""

        spec = parse("$.items[*][?(@.score > %d)]")

        item1 = DictContext({"name": "Alice", "score": 90})
        item2 = DictContext({"name": "Bob", "score": 75})
        item3 = DictContext({"name": "Charlie", "score": 85})

        collection = CollectionContext([item1, item2, item3])
        root = DictContext({"items": collection})

        # At least one item has score > 80
        self.assertTrue(spec.match(root, (80,)))

        # No items have score > 95
        self.assertFalse(spec.match(root, (95,)))

    def test_wildcard_with_named_placeholder(self):
        """Test wildcard with named placeholder."""

        spec = parse("$.users[*][?(@.age >= %(min_age)d)]")

        user1 = DictContext({"name": "Alice", "age": 30})
        user2 = DictContext({"name": "Bob", "age": 25})

        collection = CollectionContext([user1, user2])
        root = DictContext({"users": collection})

        self.assertTrue(spec.match(root, {"min_age": 28}))
        self.assertFalse(spec.match(root, {"min_age": 35}))

    def test_wildcard_string_comparison(self):
        """Test wildcard with string comparison."""

        spec = parse("$.users[*][?@.role == %s]")

        user1 = DictContext({"name": "Alice", "role": "admin"})
        user2 = DictContext({"name": "Bob", "role": "user"})

        collection = CollectionContext([user1, user2])
        root = DictContext({"users": collection})

        self.assertTrue(spec.match(root, ("admin",)))
        self.assertFalse(spec.match(root, ("guest",)))

    def test_error_on_non_context_data(self):
        """Test error when data doesn't implement Context protocol."""
        spec = parse("$[?(@.age > %d)]")

        class NoGetMethod:
            def __init__(self):
                self.age = 30

        invalid_data = NoGetMethod()

        with self.assertRaises(TypeError):
            spec.match(invalid_data, (25,))

    def test_error_on_missing_field(self):
        """Test error when field doesn't exist."""
        spec = parse("$[?(@.age > %d)]")
        user = DictContext({"name": "Alice"})  # No age field

        with self.assertRaises(KeyError):
            spec.match(user, (25,))

    def test_logical_and_operator(self):
        """Test logical AND operator (RFC 9535: &&)."""
        spec = parse("$[?@.age > %d && @.active == %s]")
        user = DictContext({"age": 30, "active": True})

        self.assertTrue(spec.match(user, (25, True)))
        self.assertFalse(spec.match(user, (35, True)))
        self.assertFalse(spec.match(user, (25, False)))

    def test_logical_or_operator(self):
        """Test logical OR operator (RFC 9535: ||)."""
        spec = parse("$[?@.age < %d || @.age > %d]")
        user_young = DictContext({"age": 15})
        user_old = DictContext({"age": 70})
        user_middle = DictContext({"age": 40})

        self.assertTrue(spec.match(user_young, (18, 65)))
        self.assertTrue(spec.match(user_old, (18, 65)))
        self.assertFalse(spec.match(user_middle, (18, 65)))

    def test_logical_not_operator(self):
        """Test logical NOT operator (RFC 9535: !)."""
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

    def test_rfc9535_equality_operator(self):
        """Test RFC 9535 equality operator (==)."""
        spec = parse("$[?@.age == %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))
        self.assertFalse(spec.match(user, (25,)))

    def test_boolean_values(self):
        """Test boolean values in comparisons."""
        spec = parse("$[?@.active == %s]")
        user_active = DictContext({"active": True})
        user_inactive = DictContext({"active": False})

        self.assertTrue(spec.match(user_active, (True,)))
        self.assertFalse(spec.match(user_active, (False,)))
        self.assertTrue(spec.match(user_inactive, (False,)))
        self.assertFalse(spec.match(user_inactive, (True,)))


class TestLexer(unittest.TestCase):
    """Test the lexer component."""

    def test_tokenize_simple_expression(self):
        """Test tokenizing a simple expression."""

        lexer = Lexer("$[?(@.age > 25)]")
        tokens = lexer.tokenize()

        # Verify we have tokens
        self.assertGreater(len(tokens), 0)

        # Verify token types
        token_types = [t.type for t in tokens]
        self.assertIn("DOLLAR", token_types)
        self.assertIn("AT", token_types)
        self.assertIn("IDENTIFIER", token_types)
        self.assertIn("GT", token_types)
        self.assertIn("NUMBER", token_types)

    def test_tokenize_with_placeholder(self):
        """Test tokenizing with placeholder."""

        lexer = Lexer("$[?(@.age > %d)]")
        tokens = lexer.tokenize()

        token_types = [t.type for t in tokens]
        self.assertIn("PLACEHOLDER", token_types)

    def test_tokenize_named_placeholder(self):
        """Test tokenizing with named placeholder."""

        lexer = Lexer("$[?(@.age > %(min_age)d)]")
        tokens = lexer.tokenize()

        # Find placeholder token
        placeholder_tokens = [t for t in tokens if t.type == "PLACEHOLDER"]
        self.assertEqual(len(placeholder_tokens), 1)
        self.assertEqual(placeholder_tokens[0].value, "%(min_age)d")


class TestNativeParserNestedWildcards(unittest.TestCase):
    """Test nested wildcard functionality in native parser."""

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

    def test_nested_wildcard_with_named_placeholder(self):
        """Test nested wildcard with named placeholder."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %(min_price)f]]")

        # Create test data
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        items = CollectionContext([item1])
        category = DictContext({"name": "Electronics", "items": items})

        categories = CollectionContext([category])
        store = DictContext({"categories": categories})

        # Should match with named parameter
        self.assertTrue(spec.match(store, {"min_price": 500.0}))
        self.assertFalse(spec.match(store, {"min_price": 1000.0}))


class TestNativeParserNestedPaths(unittest.TestCase):
    """Test nested path functionality in native parser."""

    def test_nested_path_simple(self):
        """Test simple nested path: $.a.b.c[?@.x > value]"""
        spec = parse("$.store.products[*][?@.price > %f]")

        # Create nested structure
        product1 = DictContext({"name": "Laptop", "price": 999.0})
        product2 = DictContext({"name": "Mouse", "price": 29.0})
        products = CollectionContext([product1, product2])

        data = NestedDictContext({
            "store": {
                "name": "MyStore",
                "products": products
            }
        })

        # Should match: laptop price > 500
        self.assertTrue(spec.match(data, (500.0,)))

        # Should not match: no products > 1000
        self.assertFalse(spec.match(data, (1000.0,)))

    def test_nested_path_deep(self):
        """Test deep nested path: $.a.b.c.d[?@.field > value]"""
        spec = parse("$.company.department.team.members[*][?@.age > %d]")

        member1 = DictContext({"name": "Alice", "age": 30})
        member2 = DictContext({"name": "Bob", "age": 25})
        members = CollectionContext([member1, member2])

        data = NestedDictContext({
            "company": {
                "name": "TechCorp",
                "department": {
                    "name": "Engineering",
                    "team": {
                        "name": "Backend",
                        "members": members
                    }
                }
            }
        })

        # Should match: Alice age > 28
        self.assertTrue(spec.match(data, (28,)))

        # Should not match: no members > 35
        self.assertFalse(spec.match(data, (35,)))

    def test_nested_path_in_filter(self):
        """Test nested path in filter expression: $[?@.a.b.c > value]"""
        spec = parse("$[?@.user.profile.age > %d]")

        data = NestedDictContext({
            "user": {
                "name": "Alice",
                "profile": {
                    "age": 30,
                    "city": "NYC"
                }
            }
        })

        # Should match: age > 25
        self.assertTrue(spec.match(data, (25,)))

        # Should not match: age not > 35
        self.assertFalse(spec.match(data, (35,)))

    def test_nested_path_with_logical_operators(self):
        """Test nested path with logical operators."""
        spec = parse("$.store.products[*][?@.price > %f && @.stock > %d]")

        product1 = DictContext({"name": "Laptop", "price": 999.0, "stock": 5})
        product2 = DictContext({"name": "Mouse", "price": 29.0, "stock": 100})
        product3 = DictContext({"name": "Monitor", "price": 599.0, "stock": 10})
        products = CollectionContext([product1, product2, product3])

        data = NestedDictContext({
            "store": {
                "products": products
            }
        })

        # Should match: Monitor (price > 500 && stock > 5)
        self.assertTrue(spec.match(data, (500.0, 5)))

        # Should not match: no products with price > 1000
        self.assertFalse(spec.match(data, (1000.0, 1)))

    def test_nested_path_with_named_placeholder(self):
        """Test nested path with named placeholder."""
        spec = parse("$.warehouse.items[*][?@.quantity < %(min_qty)d]")

        item1 = DictContext({"name": "Widget", "quantity": 5})
        item2 = DictContext({"name": "Gadget", "quantity": 50})
        items = CollectionContext([item1, item2])

        data = NestedDictContext({
            "warehouse": {
                "location": "East",
                "items": items
            }
        })

        # Should match: Widget quantity < 10
        self.assertTrue(spec.match(data, {"min_qty": 10}))

        # Should not match: no items < 3
        self.assertFalse(spec.match(data, {"min_qty": 3}))

    def test_deeply_nested_filter_field(self):
        """Test deeply nested field in filter expression."""
        spec = parse("$[?@.company.department.manager.level > %d]")

        data = NestedDictContext({
            "company": {
                "name": "TechCorp",
                "department": {
                    "name": "Engineering",
                    "manager": {
                        "name": "Alice",
                        "level": 5
                    }
                }
            }
        })

        # Should match: manager level > 3
        self.assertTrue(spec.match(data, (3,)))

        # Should not match: manager level not > 10
        self.assertFalse(spec.match(data, (10,)))


if __name__ == "__main__":
    unittest.main()
