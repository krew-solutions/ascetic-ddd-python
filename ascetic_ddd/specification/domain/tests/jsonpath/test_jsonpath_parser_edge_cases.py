"""Unit tests of the native JSONPath parser: edge cases, reuse, concurrency.

Written for a parser that converted the tree of the jsonpath2 library, which
was removed: the template language has one parser. The tests of the language
passed against the native parser as they stood and are kept; the tests of the
library's own spelling of equality, `=`, went with it.
"""
import threading
import unittest
from typing import Any

from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    parse,
    JSONPathSyntaxError,
    JSONPathTypeError,
)
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext
from ascetic_ddd.specification.domain.nodes import And, Or, Equal, Not, GreaterThan


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


class TestParser(unittest.TestCase):
    """Test JSONPath parser."""

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

    def test_simple_comparison_not_equal(self):
        """Test simple not-equal comparison."""
        spec = parse("$[?(@.status != %s)]")
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

    def test_error_on_non_context_data(self):
        """Test error when data doesn't implement Context protocol."""
        spec = parse("$[?(@.age > %d)]")

        class NoGetMethod:
            def __init__(self):
                self.age = 30

        invalid_data = NoGetMethod()

        with self.assertRaises(JSONPathTypeError):
            spec.match(invalid_data, (25,))

    def test_error_on_missing_field(self):
        """Test error when field doesn't exist."""
        spec = parse("$[?(@.age > %d)]")
        user = DictContext({"name": "Alice"})  # No age field

        with self.assertRaises(KeyError):
            spec.match(user, (25,))

    def test_logical_and_operator(self):
        """Test logical AND operator with && syntax (normalized to 'and')."""
        spec = parse("$[?(@.age > %d && @.active == %s)]")

        active_user = DictContext({"name": "Alice", "age": 30, "active": True})
        young_active_user = DictContext({"name": "Charlie", "age": 20, "active": True})

        self.assertTrue(spec.match(active_user, (25, True)))
        self.assertFalse(spec.match(young_active_user, (25, True)))

    def test_logical_or_operator(self):
        """Test logical OR operator with || syntax (normalized to 'or')."""
        spec = parse("$[?(@.age < %d || @.age > %d)]")

        user_young = DictContext({"age": 15})
        user_old = DictContext({"age": 70})
        user_middle = DictContext({"age": 40})

        self.assertTrue(spec.match(user_young, (18, 65)))
        self.assertTrue(spec.match(user_old, (18, 65)))
        self.assertFalse(spec.match(user_middle, (18, 65)))

    def test_multiple_positional_placeholders(self):
        """Test multiple positional placeholders with && operator."""
        spec = parse("$[?(@.age > %d && @.score > %f)]")

        user = DictContext({"age": 30, "score": 85.5})

        self.assertTrue(spec.match(user, (25, 80.0)))
        self.assertFalse(spec.match(user, (35, 80.0)))
        self.assertFalse(spec.match(user, (25, 90.0)))

    def test_mixed_placeholders(self):
        """Test that named and positional placeholders are not mixed.

        A template that mixes them has no parameters it could be matched
        with, and is refused when parsed; one style by itself works.
        """
        with self.assertRaises(JSONPathSyntaxError):
            parse("$[?(@.age > %(min_age)d && @.score > %f)]")

        spec = parse("$[?(@.age > %(min_age)d)]")

        user = DictContext({"age": 30, "score": 85.5})

        self.assertTrue(spec.match(user, {"min_age": 25}))
        self.assertFalse(spec.match(user, {"min_age": 35}))


class TestParserEdgeCases(unittest.TestCase):
    """Test edge cases of the parser."""

    def test_integer_vs_float(self):
        """Test that integer and float comparisons work correctly."""
        spec_int = parse("$[?(@.value > %d)]")
        spec_float = parse("$[?(@.value > %f)]")

        obj = DictContext({"value": 100})

        self.assertTrue(spec_int.match(obj, (99,)))
        self.assertTrue(spec_float.match(obj, (99.5,)))

    def test_double_equals_with_numbers(self):
        """Test == normalization with numeric comparisons."""
        spec = parse("$[?(@.age == %d)]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (30,)))
        self.assertFalse(spec.match(user, (25,)))

    def test_double_equals_in_string_literal_preserved(self):
        """Test that == inside string literals is not replaced."""
        spec = parse("$[?(@.value == %s)]")
        obj = DictContext({"value": "test=="})

        # The == in the string "test==" should be preserved
        self.assertTrue(spec.match(obj, ("test==",)))

    def test_logical_and_operator_normalization(self):
        """Test && operator normalization to 'and'."""
        spec = parse("$[?(@.age > %d && @.active == %s)]")

        user_match = DictContext({"age": 30, "active": True})
        user_no_match_age = DictContext({"age": 20, "active": True})
        user_no_match_active = DictContext({"age": 30, "active": False})

        self.assertTrue(spec.match(user_match, (25, True)))
        self.assertFalse(spec.match(user_no_match_age, (25, True)))
        self.assertFalse(spec.match(user_no_match_active, (25, True)))

    def test_logical_or_operator_normalization(self):
        """Test || operator normalization to 'or'."""
        spec = parse("$[?(@.age > %d || @.score > %d)]")

        user_age = DictContext({"age": 30, "score": 70})
        user_score = DictContext({"age": 20, "score": 90})
        user_both = DictContext({"age": 30, "score": 90})
        user_neither = DictContext({"age": 20, "score": 70})

        self.assertTrue(spec.match(user_age, (25, 80)))
        self.assertTrue(spec.match(user_score, (25, 80)))
        self.assertTrue(spec.match(user_both, (25, 80)))
        self.assertFalse(spec.match(user_neither, (25, 80)))

    def test_logical_not_operator_normalization(self):
        """Test ! operator normalization to 'not'."""
        spec = parse("$[?(!(@.active == %s))]")

        user_active = DictContext({"active": True})
        user_inactive = DictContext({"active": False})

        self.assertFalse(spec.match(user_active, (True,)))
        self.assertTrue(spec.match(user_inactive, (True,)))

    def test_not_operator_does_not_affect_not_equal(self):
        """Test that ! normalization doesn't affect != operator."""
        spec = parse("$[?(@.status != %s)]")

        user = DictContext({"status": "active"})

        self.assertTrue(spec.match(user, ("inactive",)))
        self.assertFalse(spec.match(user, ("active",)))

    def test_complex_logical_expression(self):
        """Test complex expression with nested AND/OR."""
        # Test: age > 25 AND (active OR score > 80)
        spec = parse("$[?(@.age > %d && (@.active == %s || @.score > %d))]")

        user1 = DictContext({"age": 30, "active": True, "score": 70})
        user2 = DictContext({"age": 30, "active": False, "score": 90})
        user3 = DictContext({"age": 20, "active": True, "score": 90})

        self.assertTrue(spec.match(user1, (25, True, 80)))  # age and active
        self.assertTrue(spec.match(user2, (25, True, 80)))  # age and score
        self.assertFalse(spec.match(user3, (25, True, 80)))  # age fails

    def test_logical_operators_in_string_literals_preserved(self):
        """Test that &&, ||, ! inside strings are not replaced."""
        spec = parse("$[?(@.value == %s)]")

        obj_and = DictContext({"value": "test&&value"})
        obj_or = DictContext({"value": "test||value"})
        obj_not = DictContext({"value": "test!value"})

        self.assertTrue(spec.match(obj_and, ("test&&value",)))
        self.assertTrue(spec.match(obj_or, ("test||value",)))
        self.assertTrue(spec.match(obj_not, ("test!value",)))


class TestNestedPaths(unittest.TestCase):
    """Test nested paths functionality."""

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

    def test_auto_parentheses(self):
        """Test that a filter needs no parentheses around its condition."""
        # Without parentheses - the same as with them
        spec = parse("$[?@.age > %d]")
        user = DictContext({"age": 30})

        self.assertTrue(spec.match(user, (25,)))
        self.assertFalse(spec.match(user, (35,)))


class TestNestedWildcards(unittest.TestCase):
    """Test nested wildcard functionality."""

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

        item3 = DictContext({"name": "Monitor", "price": 599.0})
        items2 = CollectionContext([item3])
        category2 = DictContext({"name": "Displays", "items": items2})

        categories = CollectionContext([category1, category2])
        store = DictContext({"categories": categories})

        # Should match: Monitor is 500 < price < 700
        self.assertTrue(spec.match(store, (500.0, 700.0)))

        # Should not match: no items with 100 < price < 200
        self.assertFalse(spec.match(store, (100.0, 200.0)))

    def test_nested_wildcard_empty_collection(self):
        """Test nested wildcard with empty inner collection."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

        # Category with empty items
        empty_items = CollectionContext([])
        category = DictContext({"name": "Empty", "items": empty_items})
        categories = CollectionContext([category])
        store = DictContext({"categories": categories})

        # Should not match: no items at all
        self.assertFalse(spec.match(store, (100.0,)))

    def test_nested_wildcard_multiple_matches(self):
        """Test nested wildcard where multiple categories match."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

        # Both categories have items with price > 500
        item1 = DictContext({"name": "Laptop", "price": 999.0})
        items1 = CollectionContext([item1])
        category1 = DictContext({"name": "Electronics", "items": items1})

        item2 = DictContext({"name": "Designer Shirt", "price": 599.0})
        items2 = CollectionContext([item2])
        category2 = DictContext({"name": "Clothing", "items": items2})

        categories = CollectionContext([category1, category2])
        store = DictContext({"categories": categories})

        # Should match: both categories have items > 500
        self.assertTrue(spec.match(store, (500.0,)))

    def test_nested_wildcard_with_named_placeholder(self):
        """Test nested wildcard with named placeholder."""
        spec = parse("$.categories[*][?@.items[*][?@.price > %(min_price)f]]")

        item1 = DictContext({"name": "Laptop", "price": 999.0})
        items1 = CollectionContext([item1])
        category1 = DictContext({"name": "Electronics", "items": items1})

        categories = CollectionContext([category1])
        store = DictContext({"categories": categories})

        self.assertTrue(spec.match(store, {"min_price": 500.0}))
        self.assertFalse(spec.match(store, {"min_price": 1000.0}))


class TestOperatorAssociativity(unittest.TestCase):
    """
    Test operator associativity.

    These tests verify that operators are left-associative in the resulting AST:
    - a && b && c should produce And(And(a, b), c), not And(a, And(b, c))
    - a || b || c should produce Or(Or(a, b), c), not Or(a, Or(b, c))

    Note: the parser builds a left-associative binary tree.
    """

    def _get_ast(self, spec, data, params):
        """Helper to get the AST from a spec by triggering match()."""


        return spec.bind(params)

    def test_and_left_associativity(self):
        """Test that && produces left-associative And tree."""
        spec = parse("$[?(@.a == %d && @.b == %d && @.c == %d)]")

        ast = self._get_ast(spec, None, (1, 2, 3))

        # Top level should be And
        self.assertIsInstance(ast, And)
        # Left child should also be And (left-associative)
        self.assertIsInstance(ast.left(), And)
        # Right child should be Equal (the last comparison)
        self.assertIsInstance(ast.right(), Equal)

    def test_or_left_associativity(self):
        """Test that || produces left-associative Or tree."""
        spec = parse("$[?(@.a == %d || @.b == %d || @.c == %d)]")

        ast = self._get_ast(spec, None, (1, 2, 3))

        # Top level should be Or
        self.assertIsInstance(ast, Or)
        # Left child should also be Or (left-associative)
        self.assertIsInstance(ast.left(), Or)
        # Right child should be Equal
        self.assertIsInstance(ast.right(), Equal)

    def test_functional_and_chain(self):
        """Functional test for AND chain."""
        spec = parse("$[?(@.a == %d && @.b == %d && @.c == %d)]")

        data_all_match = DictContext({"a": 1, "b": 2, "c": 3})
        data_first_fails = DictContext({"a": 0, "b": 2, "c": 3})
        data_middle_fails = DictContext({"a": 1, "b": 0, "c": 3})
        data_last_fails = DictContext({"a": 1, "b": 2, "c": 0})

        self.assertTrue(spec.match(data_all_match, (1, 2, 3)))
        self.assertFalse(spec.match(data_first_fails, (1, 2, 3)))
        self.assertFalse(spec.match(data_middle_fails, (1, 2, 3)))
        self.assertFalse(spec.match(data_last_fails, (1, 2, 3)))

    def test_functional_or_chain(self):
        """Functional test for OR chain."""
        spec = parse("$[?(@.a == %d || @.b == %d || @.c == %d)]")

        data_all_match = DictContext({"a": 1, "b": 2, "c": 3})
        data_first_matches = DictContext({"a": 1, "b": 0, "c": 0})
        data_middle_matches = DictContext({"a": 0, "b": 2, "c": 0})
        data_last_matches = DictContext({"a": 0, "b": 0, "c": 3})
        data_none_match = DictContext({"a": 0, "b": 0, "c": 0})

        self.assertTrue(spec.match(data_all_match, (1, 2, 3)))
        self.assertTrue(spec.match(data_first_matches, (1, 2, 3)))
        self.assertTrue(spec.match(data_middle_matches, (1, 2, 3)))
        self.assertTrue(spec.match(data_last_matches, (1, 2, 3)))
        self.assertFalse(spec.match(data_none_match, (1, 2, 3)))


class TestOperatorPrecedence(unittest.TestCase):
    """
    Test operator precedence.

    && binds tighter than ||, as in RFC 9535. The jsonpath2 library these
    tests were written for read the two left to right at one level, so they
    controlled grouping with explicit parentheses and asserted nothing of a
    template without them; `test_parentheses_change_semantics` now does.
    """

    def _get_ast(self, spec, params):
        """Helper to get the AST from a spec."""


        return spec.bind(params)

    def test_explicit_grouping_and_in_or(self):
        """Test explicit grouping: a || (b && c)."""
        spec = parse("$[?(@.a == %d || (@.b == %d && @.c == %d))]")

        # a=1, b=0, c=0 -> True (a matches)
        data1 = DictContext({"a": 1, "b": 0, "c": 0})
        self.assertTrue(spec.match(data1, (1, 2, 3)))

        # a=0, b=2, c=3 -> True (b && c matches)
        data2 = DictContext({"a": 0, "b": 2, "c": 3})
        self.assertTrue(spec.match(data2, (1, 2, 3)))

        # a=0, b=2, c=0 -> False (a fails, c fails)
        data3 = DictContext({"a": 0, "b": 2, "c": 0})
        self.assertFalse(spec.match(data3, (1, 2, 3)))

    def test_explicit_grouping_or_in_and(self):
        """Test explicit grouping: (a || b) && c."""
        spec = parse("$[?((@.a == %d || @.b == %d) && @.c == %d)]")

        # a=1, b=0, c=3 -> True (a || b matches, c matches)
        data1 = DictContext({"a": 1, "b": 0, "c": 3})
        self.assertTrue(spec.match(data1, (1, 2, 3)))

        # a=0, b=2, c=3 -> True (b matches in first group, c matches)
        data2 = DictContext({"a": 0, "b": 2, "c": 3})
        self.assertTrue(spec.match(data2, (1, 2, 3)))

        # a=1, b=0, c=0 -> False (a || b matches, but c fails)
        data3 = DictContext({"a": 1, "b": 0, "c": 0})
        self.assertFalse(spec.match(data3, (1, 2, 3)))

        # a=0, b=0, c=3 -> False (a || b fails)
        data4 = DictContext({"a": 0, "b": 0, "c": 3})
        self.assertFalse(spec.match(data4, (1, 2, 3)))

    def test_simple_and_chain(self):
        """Test simple AND chain: a && b && c."""
        spec = parse("$[?(@.a == %d && @.b == %d && @.c == %d)]")

        # All match
        data1 = DictContext({"a": 1, "b": 2, "c": 3})
        self.assertTrue(spec.match(data1, (1, 2, 3)))

        # First fails
        data2 = DictContext({"a": 0, "b": 2, "c": 3})
        self.assertFalse(spec.match(data2, (1, 2, 3)))

        # Middle fails
        data3 = DictContext({"a": 1, "b": 0, "c": 3})
        self.assertFalse(spec.match(data3, (1, 2, 3)))

        # Last fails
        data4 = DictContext({"a": 1, "b": 2, "c": 0})
        self.assertFalse(spec.match(data4, (1, 2, 3)))

    def test_simple_or_chain(self):
        """Test simple OR chain: a || b || c."""
        spec = parse("$[?(@.a == %d || @.b == %d || @.c == %d)]")

        # First matches
        data1 = DictContext({"a": 1, "b": 0, "c": 0})
        self.assertTrue(spec.match(data1, (1, 2, 3)))

        # Middle matches
        data2 = DictContext({"a": 0, "b": 2, "c": 0})
        self.assertTrue(spec.match(data2, (1, 2, 3)))

        # Last matches
        data3 = DictContext({"a": 0, "b": 0, "c": 3})
        self.assertTrue(spec.match(data3, (1, 2, 3)))

        # None match
        data4 = DictContext({"a": 0, "b": 0, "c": 0})
        self.assertFalse(spec.match(data4, (1, 2, 3)))

    def test_parentheses_change_semantics(self):
        """Test that parentheses change evaluation semantics."""
        # Without grouping: a && b || c is (a && b) || c
        spec1 = parse("$[?(@.a == %d && @.b == %d || @.c == %d)]")

        # With explicit grouping: a && (b || c)
        spec2 = parse("$[?(@.a == %d && (@.b == %d || @.c == %d))]")

        # Data where a=1, b=0, c=3
        data = DictContext({"a": 1, "b": 0, "c": 3})

        # spec2 should match: a && (b || c) = 1 && (0 || 3) = 1 && 1 = True
        self.assertTrue(spec2.match(data, (1, 2, 3)))
        # spec1 should match too: (a && b) || c = (1 && 0) || 1 = True
        self.assertTrue(spec1.match(data, (1, 2, 3)))

        # Data where a=0, b=0, c=3: the two differ
        data = DictContext({"a": 0, "b": 0, "c": 3})
        # (a && b) || c = (0 && 0) || 1 = True
        self.assertTrue(spec1.match(data, (1, 2, 3)))
        # a && (b || c) = 0 && (0 || 1) = False
        self.assertFalse(spec2.match(data, (1, 2, 3)))


class TestErrorHandling(unittest.TestCase):
    """
    Test error handling.

    These tests verify that appropriate errors are raised
    for invalid inputs and edge cases.
    """

    def test_invalid_context_type_error(self):
        """Test JSONPathTypeError for invalid context object."""
        spec = parse("$[?(@.age > %d)]")

        class InvalidContext:
            pass

        with self.assertRaises(JSONPathTypeError) as ctx:
            spec.match(InvalidContext(), (25,))

        self.assertIn("Context", str(ctx.exception))

    def test_missing_positional_parameter(self):
        """Test error when positional parameter is missing."""
        spec = parse("$[?(@.age > %d && @.score > %d)]")
        user = DictContext({"age": 30, "score": 85})

        with self.assertRaises(JSONPathSyntaxError):
            spec.match(user, (25,))  # Missing second parameter

    def test_missing_named_parameter(self):
        """Test error when named parameter is missing."""
        spec = parse("$[?(@.age > %(min_age)d)]")
        user = DictContext({"age": 30})

        with self.assertRaises(JSONPathSyntaxError):
            spec.match(user, {"wrong_name": 25})

    def test_missing_field_in_data(self):
        """Test KeyError when field doesn't exist in data."""
        spec = parse("$[?(@.nonexistent > %d)]")
        user = DictContext({"age": 30})

        with self.assertRaises(KeyError):
            spec.match(user, (25,))

    def test_invalid_jsonpath_syntax(self):
        """Test error on invalid JSONPath syntax."""
        # Refused when parsed, before there is anything to match
        with self.assertRaises(JSONPathSyntaxError):
            parse("$[?(@.age >< %d)]")  # Invalid operator

    def test_error_message_contains_type_info(self):
        """Test that type error contains useful type information."""
        spec = parse("$[?(@.age > %d)]")

        class MyCustomClass:
            pass

        try:
            spec.match(MyCustomClass(), (25,))
            self.fail("Expected JSONPathTypeError")
        except JSONPathTypeError as e:
            message = str(e)
            self.assertIn("MyCustomClass", message)
            self.assertIn("get", message.lower())


class TestThreadSafety(unittest.TestCase):
    """
    Test thread safety.

    These tests verify that a single specification instance can be
    safely used from multiple threads concurrently.
    """

    def test_concurrent_match_calls(self):
        """Test that match() is thread-safe."""
        spec = parse("$[?(@.value > %d)]")
        errors = []
        results = []
        lock = threading.Lock()

        def worker(thread_id, iterations=100):
            try:
                for i in range(iterations):
                    data = DictContext({"value": thread_id * 10 + i})
                    result = spec.match(data, (thread_id * 10,))
                    with lock:
                        results.append((thread_id, i, result))
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(len(results), 1000)

    def test_concurrent_different_params(self):
        """Test concurrent calls with different parameters."""
        spec = parse("$[?(@.x == %d)]")
        errors = []
        results = []
        lock = threading.Lock()

        def worker(thread_id):
            try:
                for i in range(50):
                    data = DictContext({"x": thread_id})
                    result = spec.match(data, (thread_id,))
                    with lock:
                        results.append((thread_id, result))
                    # All should match since we use the same values
                    if not result:
                        with lock:
                            errors.append((thread_id, "Unexpected False result"))
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors: {errors}")
        self.assertTrue(all(r[1] for r in results))

    def test_concurrent_complex_expressions(self):
        """Test concurrent calls with complex expressions."""
        spec = parse("$[?(@.a > %d && @.b < %d || @.c == %s)]")
        errors = []
        results = []
        lock = threading.Lock()

        def worker(thread_id):
            try:
                for i in range(30):
                    data = DictContext({
                        "a": thread_id + i,
                        "b": 100 - thread_id - i,
                        "c": f"val_{thread_id}"
                    })
                    # Should match when a > 10, b < 90, or c == "val_{thread_id}"
                    result = spec.match(data, (10, 90, f"val_{thread_id}"))
                    with lock:
                        results.append((thread_id, i, result))
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors: {errors}")
        self.assertEqual(len(results), 240)


class TestBoundaryConditions(unittest.TestCase):
    """
    Test boundary conditions and edge cases.

    These tests verify correct behavior for edge cases that
    might not be covered by typical usage patterns.
    """

    def test_empty_string_comparison(self):
        """Test comparison with empty string."""
        spec = parse("$[?(@.name == %s)]")

        data_empty = DictContext({"name": ""})
        data_nonempty = DictContext({"name": "Alice"})

        self.assertTrue(spec.match(data_empty, ("",)))
        self.assertFalse(spec.match(data_nonempty, ("",)))

    def test_zero_comparison(self):
        """Test comparison with zero values."""
        spec = parse("$[?(@.value > %d)]")

        data_zero = DictContext({"value": 0})
        data_negative = DictContext({"value": -5})
        data_positive = DictContext({"value": 5})

        self.assertFalse(spec.match(data_zero, (0,)))  # 0 > 0 is False
        self.assertFalse(spec.match(data_negative, (0,)))  # -5 > 0 is False
        self.assertTrue(spec.match(data_positive, (0,)))  # 5 > 0 is True

    def test_negative_numbers(self):
        """Test comparison with negative numbers."""
        spec = parse("$[?(@.value > %d)]")

        data = DictContext({"value": -5})

        self.assertTrue(spec.match(data, (-10,)))  # -5 > -10
        self.assertFalse(spec.match(data, (0,)))   # -5 > 0 is False

    def test_large_numbers(self):
        """Test comparison with large numbers."""
        spec = parse("$[?(@.value > %d)]")

        data = DictContext({"value": 10**15})

        self.assertTrue(spec.match(data, (10**14,)))
        self.assertFalse(spec.match(data, (10**16,)))

    def test_float_precision(self):
        """Test float comparison precision."""
        spec = parse("$[?(@.value > %f)]")

        data = DictContext({"value": 0.1 + 0.2})  # ~0.30000000000000004

        self.assertTrue(spec.match(data, (0.29,)))
        self.assertTrue(spec.match(data, (0.3,)))  # Due to float precision

    def test_none_value(self):
        """Test comparison with None value."""
        spec = parse("$[?(@.value == %s)]")

        data_none = DictContext({"value": None})

        self.assertTrue(spec.match(data_none, (None,)))

    def test_boolean_false_vs_none(self):
        """Test distinguishing False from None."""
        spec = parse("$[?(@.value == %s)]")

        data_false = DictContext({"value": False})
        data_none = DictContext({"value": None})

        self.assertTrue(spec.match(data_false, (False,)))
        self.assertFalse(spec.match(data_false, (None,)))
        self.assertTrue(spec.match(data_none, (None,)))
        self.assertFalse(spec.match(data_none, (False,)))

    def test_special_characters_in_string(self):
        """Test strings with special characters."""
        spec = parse("$[?(@.value == %s)]")

        # Test various special characters
        special_strings = [
            "hello\nworld",  # newline
            "hello\tworld",  # tab
            "hello\\world",  # backslash
            'hello"world',   # quote
            "hello'world",   # single quote
            "hello world",   # space
        ]

        for special in special_strings:
            data = DictContext({"value": special})
            self.assertTrue(
                spec.match(data, (special,)),
                f"Failed for string: {repr(special)}"
            )

    def test_unicode_strings(self):
        """Test Unicode string handling."""
        spec = parse("$[?(@.name == %s)]")

        unicode_names = [
            "Алиса",       # Russian
            "愛麗絲",       # Chinese
            "アリス",       # Japanese
            "🎉👍🔥",      # Emojis
        ]

        for name in unicode_names:
            data = DictContext({"name": name})
            self.assertTrue(
                spec.match(data, (name,)),
                f"Failed for Unicode string: {name}"
            )

    def test_deeply_nested_expression(self):
        """Test nested logical expression."""
        # (a && b) || (c && d)
        spec = parse("$[?((@.a == %d && @.b == %d) || (@.c == %d && @.d == %d))]")

        # First group matches (a && b)
        data1 = DictContext({"a": 1, "b": 2, "c": 0, "d": 0})
        self.assertTrue(spec.match(data1, (1, 2, 3, 4)))

        # Second group matches (c && d)
        data2 = DictContext({"a": 0, "b": 0, "c": 3, "d": 4})
        self.assertTrue(spec.match(data2, (1, 2, 3, 4)))

        # Both groups match
        data3 = DictContext({"a": 1, "b": 2, "c": 3, "d": 4})
        self.assertTrue(spec.match(data3, (1, 2, 3, 4)))

        # Neither group matches
        data4 = DictContext({"a": 1, "b": 0, "c": 3, "d": 0})
        self.assertFalse(spec.match(data4, (1, 2, 3, 4)))

        # All wrong values
        data5 = DictContext({"a": 0, "b": 0, "c": 0, "d": 0})
        self.assertFalse(spec.match(data5, (1, 2, 3, 4)))


class TestSpecificationReuse(unittest.TestCase):
    """
    Test specification reuse patterns.

    These tests verify that specifications can be safely reused
    with different data and parameters.
    """

    def test_reuse_with_different_data(self):
        """Test reusing spec with different data objects."""
        spec = parse("$[?(@.age > %d)]")

        users = [
            DictContext({"age": 20}),
            DictContext({"age": 30}),
            DictContext({"age": 40}),
        ]

        results = [spec.match(user, (25,)) for user in users]

        self.assertEqual(results, [False, True, True])

    def test_reuse_with_different_params(self):
        """Test reusing spec with different parameters."""
        spec = parse("$[?(@.age > %d)]")
        user = DictContext({"age": 30})

        params_list = [(20,), (25,), (30,), (35,)]
        results = [spec.match(user, p) for p in params_list]

        self.assertEqual(results, [True, True, False, False])

    def test_reuse_preserves_template(self):
        """Test that template is preserved after multiple uses."""
        spec = parse("$[?(@.age > %d)]")
        original_template = spec.template

        user = DictContext({"age": 30})

        # Use multiple times
        for _ in range(10):
            spec.match(user, (25,))

        self.assertEqual(spec.template, original_template)

    def test_interleaved_usage(self):
        """Test interleaved usage with different data and params."""
        spec = parse("$[?(@.value > %d)]")

        data1 = DictContext({"value": 100})
        data2 = DictContext({"value": 50})

        # Interleaved calls
        self.assertTrue(spec.match(data1, (50,)))   # 100 > 50
        self.assertFalse(spec.match(data2, (75,)))  # 50 > 75 is False
        self.assertTrue(spec.match(data2, (25,)))   # 50 > 25
        self.assertFalse(spec.match(data1, (150,))) # 100 > 150 is False


if __name__ == "__main__":
    unittest.main()
