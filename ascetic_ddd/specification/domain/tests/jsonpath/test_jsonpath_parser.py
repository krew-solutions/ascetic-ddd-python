"""Unit tests for native JSONPath parser (without external dependencies)."""
import threading
import unittest

from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    Lexer,
    NativeParametrizedSpecification,
    parse,
    JSONPathError,
    JSONPathSyntaxError,
    JSONPathTypeError,
)
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext, DictContext
from ascetic_ddd.specification.domain.nodes import And, Or, Equal, Not, GreaterThan, GlobalScope, Object


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

        with self.assertRaises(JSONPathTypeError):
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

        data = DictContext({
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

        data = DictContext({
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

        data = DictContext({
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

        data = DictContext({
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

        data = DictContext({
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

        data = DictContext({
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


class TestOperatorAssociativity(unittest.TestCase):
    """
    Test operator associativity.

    These tests verify that operators are left-associative:
    - a && b && c should be And(And(a, b), c), not And(a, And(b, c))
    - a || b || c should be Or(Or(a, b), c), not Or(a, Or(b, c))

    Without these tests, the parser could have right-associativity
    and still pass all functional tests (since the result is the same
    for boolean operations), but would violate standard semantics.
    """

    def test_and_left_associativity(self):
        """Test that && is left-associative: a && b && c -> And(And(a, b), c)"""
        spec = parse("$[?@.a == 1 && @.b == 2 && @.c == 3]")

        # Check AST structure
        ast = spec.bind()
        # Top level should be And
        self.assertIsInstance(ast, And)
        # Left child should also be And (left-associative)
        self.assertIsInstance(ast.left(), And)
        # Right child should be Equal (the last comparison)
        self.assertIsInstance(ast.right(), Equal)

        # The innermost And's children should both be Equal
        inner_and = ast.left()
        self.assertIsInstance(inner_and.left(), Equal)
        self.assertIsInstance(inner_and.right(), Equal)

    def test_or_left_associativity(self):
        """Test that || is left-associative: a || b || c -> Or(Or(a, b), c)"""
        spec = parse("$[?@.a == 1 || @.b == 2 || @.c == 3]")

        ast = spec.bind()
        # Top level should be Or
        self.assertIsInstance(ast, Or)
        # Left child should also be Or (left-associative)
        self.assertIsInstance(ast.left(), Or)
        # Right child should be Equal
        self.assertIsInstance(ast.right(), Equal)

    def test_mixed_operators_associativity(self):
        """Test mixed && and || with correct associativity."""
        # a && b || c && d should be Or(And(a, b), And(c, d))
        spec = parse("$[?@.a == 1 && @.b == 2 || @.c == 3 && @.d == 4]")

        ast = spec.bind()
        # Top level should be Or (lowest precedence)
        self.assertIsInstance(ast, Or)
        # Both children should be And
        self.assertIsInstance(ast.left(), And)
        self.assertIsInstance(ast.right(), And)


class TestOperatorPrecedence(unittest.TestCase):
    """
    Test operator precedence.

    These tests verify that && has higher precedence than ||:
    - a || b && c should be Or(a, And(b, c)), not And(Or(a, b), c)

    Without these tests, the parser could treat && and || with equal
    precedence and still pass most functional tests.
    """

    def test_and_higher_precedence_than_or(self):
        """Test that && binds tighter than ||: a || b && c -> Or(a, And(b, c))"""
        spec = parse("$[?@.a == 1 || @.b == 2 && @.c == 3]")

        ast = spec.bind()
        # Top level should be Or
        self.assertIsInstance(ast, Or)
        # Left should be simple Equal
        self.assertIsInstance(ast.left(), Equal)
        # Right should be And (higher precedence bound first)
        self.assertIsInstance(ast.right(), And)

    def test_and_higher_precedence_reverse(self):
        """Test precedence: a && b || c -> Or(And(a, b), c)"""
        spec = parse("$[?@.a == 1 && @.b == 2 || @.c == 3]")

        ast = spec.bind()
        # Top level should be Or
        self.assertIsInstance(ast, Or)
        # Left should be And
        self.assertIsInstance(ast.left(), And)
        # Right should be Equal
        self.assertIsInstance(ast.right(), Equal)

    def test_parentheses_override_precedence(self):
        """Test that parentheses override default precedence."""
        # (a || b) && c - parentheses force Or to bind first
        # Note: In [?...] syntax, use double parentheses for grouping
        # because outer parens in [?(...)] are part of filter syntax
        spec = parse("$[?((@.a == 1 || @.b == 2)) && @.c == 3]")

        ast = spec.bind()
        # Top level should be And (due to parentheses)
        self.assertIsInstance(ast, And)
        # Left should be Or (grouped by parentheses)
        self.assertIsInstance(ast.left(), Or)
        # Right should be Equal
        self.assertIsInstance(ast.right(), Equal)

    def test_complex_precedence(self):
        """Test complex expression: a || b && c || d && e"""
        # Should be: Or(Or(a, And(b, c)), And(d, e))
        spec = parse("$[?@.a == 1 || @.b == 2 && @.c == 3 || @.d == 4 && @.e == 5]")

        ast = spec.bind()
        # Top level Or
        self.assertIsInstance(ast, Or)
        # Left is Or
        self.assertIsInstance(ast.left(), Or)
        # Right is And
        self.assertIsInstance(ast.right(), And)

        # Left Or's right is And
        left_or = ast.left()
        self.assertIsInstance(left_or.right(), And)


class TestErrorMessages(unittest.TestCase):
    """
    Test error handling and messages.

    These tests verify that:
    1. Correct exception types are raised
    2. Error messages contain useful information
    3. Position information is accurate
    """

    def test_syntax_error_type(self):
        """Test that syntax errors raise JSONPathSyntaxError."""
        with self.assertRaises(JSONPathSyntaxError):
            parse("$[?@.age ~ 25]")  # Invalid operator

    def test_syntax_error_inherits_from_base(self):
        """Test that JSONPathSyntaxError inherits from JSONPathError."""
        with self.assertRaises(JSONPathError):
            parse("$[?@.age ~ 25]")

    def test_syntax_error_has_position(self):
        """Test that syntax error includes position information."""
        try:
            parse("$[?@.age ~ 25]")
            self.fail("Expected JSONPathSyntaxError")
        except JSONPathSyntaxError as e:
            self.assertIsNotNone(e.position)
            self.assertEqual(e.position, 9)  # Position of '~'

    def test_syntax_error_has_expression(self):
        """Test that syntax error includes original expression."""
        expr = "$[?@.age ~ 25]"
        try:
            parse(expr)
            self.fail("Expected JSONPathSyntaxError")
        except JSONPathSyntaxError as e:
            self.assertEqual(e.expression, expr)

    def test_syntax_error_has_context(self):
        """Test that syntax error includes context hint."""
        try:
            parse("$[?@.age ~ 25]")
            self.fail("Expected JSONPathSyntaxError")
        except JSONPathSyntaxError as e:
            self.assertIsNotNone(e.context)
            self.assertIn("token", e.context.lower())

    def test_syntax_error_message_formatting(self):
        """Test that error message is well-formatted."""
        try:
            parse("$[?@.age ~ 25]")
            self.fail("Expected JSONPathSyntaxError")
        except JSONPathSyntaxError as e:
            message = str(e)
            # Should contain position
            self.assertIn("9", message)
            # Should contain the expression
            self.assertIn("$[?@.age ~ 25]", message)
            # Should contain pointer
            self.assertIn("^", message)

    def test_type_error_on_invalid_context(self):
        """Test JSONPathTypeError for invalid context."""
        spec = parse("$[?@.age > %d]")

        class InvalidContext:
            pass

        with self.assertRaises(JSONPathTypeError) as ctx:
            spec.match(InvalidContext(), (25,))

        self.assertIn("Context", str(ctx.exception))

    def test_type_error_has_expected_and_got(self):
        """Test that type error includes expected and got types."""
        spec = parse("$[?@.age > %d]")

        class InvalidContext:
            pass

        try:
            spec.match(InvalidContext(), (25,))
            self.fail("Expected JSONPathTypeError")
        except JSONPathTypeError as e:
            self.assertIsNotNone(e.expected)
            self.assertIsNotNone(e.got)
            self.assertEqual(e.got, "InvalidContext")

    def test_unexpected_end_of_expression(self):
        """Test error when expression ends unexpectedly."""
        with self.assertRaises(JSONPathSyntaxError) as ctx:
            parse("$[?@.age >")

        self.assertIn("end", str(ctx.exception).lower())

    def test_missing_field_name_error(self):
        """Test error when field name is missing."""
        with self.assertRaises(JSONPathSyntaxError) as ctx:
            parse("$[?@. > 25]")

        self.assertIn("field", str(ctx.exception).lower())


class TestThreadSafety(unittest.TestCase):
    """
    Test thread safety.

    These tests verify that a single specification instance can be
    safely used from multiple threads concurrently.

    Without these tests, mutable instance state during parsing/matching
    could cause race conditions.
    """

    def test_concurrent_match_calls(self):
        """Test that match() is thread-safe."""
        spec = parse("$[?@.value > %d]")
        errors = []
        results = []

        def worker(thread_id, iterations=100):
            try:
                for i in range(iterations):
                    data = DictContext({"value": thread_id * 10 + i})
                    result = spec.match(data, (thread_id * 10,))
                    results.append((thread_id, i, result))
            except Exception as e:
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
        spec = parse("$[?@.x == %d && @.y == %s]")
        errors = []
        results = []

        def worker(thread_id):
            try:
                for i in range(50):
                    data = DictContext({"x": thread_id, "y": f"val_{thread_id}"})
                    result = spec.match(data, (thread_id, f"val_{thread_id}"))
                    results.append((thread_id, result))
                    # All should match since we use the same values
                    if not result:
                        errors.append((thread_id, "Unexpected False result"))
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors: {errors}")
        self.assertTrue(all(r[1] for r in results))


class TestASTCaching(unittest.TestCase):
    """
    Test AST caching behavior.

    These tests verify that the AST is parsed once and cached,
    not re-parsed on every match() call.
    """

    def test_ast_is_cached(self):
        """Test that what was parsed is stored after parsing."""
        spec = parse("$[?@.age > %d]")

        # AST should be cached: as a function of the parameters, which builds
        # the tree they make of the template
        self.assertTrue(hasattr(spec, "_builder"))
        self.assertTrue(callable(spec._builder.build))

    def test_ast_not_reparsed_on_match(self):
        """Test that AST is not re-created on match()."""
        spec = parse("$[?@.age > %d]")
        original_ast = spec._builder

        # Multiple match calls
        data = DictContext({"age": 30})
        spec.match(data, (25,))
        spec.match(data, (35,))
        spec.match(data, (20,))

        # AST should be the same object
        self.assertIs(spec._builder, original_ast)

    def test_different_params_same_ast(self):
        """Test that different parameters don't affect cached AST."""
        spec = parse("$[?@.value == %s]")
        original_ast = spec._builder

        data = DictContext({"value": "test"})

        # Call with different param types
        spec.match(data, ("test",))
        spec.match(data, ("other",))
        spec.match(data, ("third",))

        # AST should remain unchanged
        self.assertIs(spec._builder, original_ast)


class TestHelperMethods(unittest.TestCase):
    """
    Test helper methods for DRY compliance.

    These tests verify that helper methods work correctly
    and can be used to reduce code duplication.
    """

    def test_parse_identifier_chain_single(self):
        """Test parsing single identifier."""
        spec = NativeParametrizedSpecification("$[?@.field > 1]")
        lexer = Lexer("field")
        tokens = lexer.tokenize()

        chain, pos = spec._parse_identifier_chain(tokens, 0)

        self.assertEqual(chain, ["field"])
        self.assertEqual(pos, 1)

    def test_parse_identifier_chain_multiple(self):
        """Test parsing multiple identifiers."""
        spec = NativeParametrizedSpecification("$[?@.a.b.c > 1]")
        lexer = Lexer("a.b.c")
        tokens = lexer.tokenize()

        chain, pos = spec._parse_identifier_chain(tokens, 0)

        self.assertEqual(chain, ["a", "b", "c"])

    def test_is_wildcard_pattern_true(self):
        """Test wildcard pattern detection - positive case."""
        spec = NativeParametrizedSpecification("$[?@.x > 1]")
        lexer = Lexer("[*]")
        tokens = lexer.tokenize()

        self.assertTrue(spec._is_wildcard_pattern(tokens, 0))

    def test_is_wildcard_pattern_false(self):
        """Test wildcard pattern detection - negative case."""
        spec = NativeParametrizedSpecification("$[?@.x > 1]")
        lexer = Lexer("[?@.x]")
        tokens = lexer.tokenize()

        self.assertFalse(spec._is_wildcard_pattern(tokens, 0))

    def test_build_object_chain_empty(self):
        """Test building object chain with empty list."""


        spec = NativeParametrizedSpecification("$[?@.x > 1]")
        parent = GlobalScope()

        result = spec._build_object_chain(parent, [])

        self.assertIs(result, parent)

    def test_build_object_chain_single(self):
        """Test building object chain with single name."""


        spec = NativeParametrizedSpecification("$[?@.x > 1]")
        parent = GlobalScope()

        result = spec._build_object_chain(parent, ["field"])

        self.assertIsInstance(result, Object)

    def test_build_object_chain_multiple(self):
        """Test building object chain with multiple names."""


        spec = NativeParametrizedSpecification("$[?@.x > 1]")
        parent = GlobalScope()

        result = spec._build_object_chain(parent, ["a", "b", "c"])

        # Should be Object(Object(Object(parent, "a"), "b"), "c")
        self.assertIsInstance(result, Object)
        # Verify nesting depth by traversing
        current = result
        depth = 0
        while isinstance(current, Object):
            depth += 1
            current = current.parent()
        self.assertEqual(depth, 3)


if __name__ == "__main__":
    unittest.main()
