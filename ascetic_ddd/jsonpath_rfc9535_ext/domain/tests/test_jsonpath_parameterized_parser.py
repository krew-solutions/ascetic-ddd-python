"""Unit tests for jsonpath-rfc9535 parser with C-style placeholders."""
import unittest
from ascetic_ddd.jsonpath_rfc9535_ext.domain.jsonpath_parameterized_parser import parse


class TestPositionalPlaceholders(unittest.TestCase):
    """Test positional placeholders (%s, %d, %f)."""

    def test_parse_integer_placeholder(self):
        """Test parsing with %d placeholder."""
        expr = parse("$[?@.age > %d]")
        self.assertEqual(len(expr.placeholders), 1)
        self.assertEqual(expr.placeholders[0]['format_type'], 'd')
        self.assertTrue(expr.placeholders[0]['positional'])

    def test_parse_string_placeholder(self):
        """Test parsing with %s placeholder."""
        expr = parse("$[?@.name == %s]")
        self.assertEqual(len(expr.placeholders), 1)
        self.assertEqual(expr.placeholders[0]['format_type'], 's')

    def test_parse_float_placeholder(self):
        """Test parsing with %f placeholder."""
        expr = parse("$[?@.price > %f]")
        self.assertEqual(len(expr.placeholders), 1)
        self.assertEqual(expr.placeholders[0]['format_type'], 'f')

    def test_execute_with_integer(self):
        """Test execution with integer parameter."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        expr = parse("$[?@.age > %d]")

        results = expr.find(data, (27,))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['age'], 30)
        self.assertEqual(results[1]['age'], 35)

    def test_execute_with_string(self):
        """Test execution with string parameter."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        expr = parse("$[?@.name == %s]")

        results = expr.find(data, ("Alice",))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Alice")

    def test_reuse_with_different_values(self):
        """Test reusing expression with different parameter values."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        expr = parse("$[?@.age > %d]")

        # Execute multiple times
        results1 = expr.find(data, (26,))
        self.assertEqual(len(results1), 2)

        results2 = expr.find(data, (30,))
        self.assertEqual(len(results2), 1)
        self.assertEqual(results2[0]['age'], 35)

        results3 = expr.find(data, (40,))
        self.assertEqual(len(results3), 0)

    def test_execute_with_float(self):
        """Test execution with float parameter."""
        data = [{"price": 10.5}, {"price": 20.75}, {"price": 5.25}]
        expr = parse("$[?@.price > %f]")

        results = expr.find(data, (10.0,))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['price'], 10.5)
        self.assertEqual(results[1]['price'], 20.75)


class TestNamedPlaceholders(unittest.TestCase):
    """Test named placeholders %(name)s, %(age)d, %(price)f."""

    def test_parse_named_integer(self):
        """Test parsing with %(name)d placeholder."""
        expr = parse("$[?@.age > %(min_age)d]")
        self.assertEqual(len(expr.placeholders), 1)
        self.assertEqual(expr.placeholders[0]['name'], 'min_age')
        self.assertEqual(expr.placeholders[0]['format_type'], 'd')
        self.assertFalse(expr.placeholders[0]['positional'])

    def test_parse_named_string(self):
        """Test parsing with %(name)s placeholder."""
        expr = parse("$[?@.name == %(username)s]")
        self.assertEqual(len(expr.placeholders), 1)
        self.assertEqual(expr.placeholders[0]['name'], 'username')
        self.assertEqual(expr.placeholders[0]['format_type'], 's')

    def test_execute_with_named_integer(self):
        """Test execution with named integer parameter."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        expr = parse("$[?@.age > %(min_age)d]")

        results = expr.find(data, {"min_age": 27})
        self.assertEqual(len(results), 2)

    def test_execute_with_named_string(self):
        """Test execution with named string parameter."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        expr = parse("$[?@.name == %(name)s]")

        results = expr.find(data, {"name": "Bob"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Bob")

    def test_reuse_with_different_named_values(self):
        """Test reusing expression with different named parameter values."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        expr = parse("$[?@.name == %(name)s]")

        for name in ["Alice", "Bob", "Charlie"]:
            results = expr.find(data, {"name": name})
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['name'], name)

    def test_execute_with_named_float(self):
        """Test execution with named float parameter."""
        data = [{"price": 10.5}, {"price": 20.75}, {"price": 5.25}]
        expr = parse("$[?@.price > %(min_price)f]")

        results = expr.find(data, {"min_price": 10.0})
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['price'], 10.5)
        self.assertEqual(results[1]['price'], 20.75)


class TestMixedCases(unittest.TestCase):
    """Test edge cases and special scenarios."""

    def test_find_one_method(self):
        """Test find_one() method."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        expr = parse("$[?@.age > %d]")

        result = expr.find_one(data, (27,))
        self.assertIsNotNone(result)
        self.assertEqual(result['age'], 30)

    def test_find_one_no_match(self):
        """Test find_one() when no matches."""
        data = [{"age": 30}]
        expr = parse("$[?@.age > %d]")

        result = expr.find_one(data, (40,))
        self.assertIsNone(result)

    def test_empty_data(self):
        """Test with empty data."""
        expr = parse("$[?@.age > %d]")
        results = expr.find([], (25,))
        self.assertEqual(len(results), 0)

    def test_comparison_operators(self):
        """Test different comparison operators."""
        data = [{"val": 10}, {"val": 20}, {"val": 30}]

        # Greater than
        expr_gt = parse("$[?@.val > %d]")
        self.assertEqual(len(expr_gt.find(data, (15,))), 2)

        # Less than
        expr_lt = parse("$[?@.val < %d]")
        self.assertEqual(len(expr_lt.find(data, (25,))), 2)

        # Greater than or equal
        expr_gte = parse("$[?@.val >= %d]")
        self.assertEqual(len(expr_gte.find(data, (20,))), 2)

        # Less than or equal
        expr_lte = parse("$[?@.val <= %d]")
        self.assertEqual(len(expr_lte.find(data, (20,))), 2)

        # Not equal
        expr_ne = parse("$[?@.val != %d]")
        self.assertEqual(len(expr_ne.find(data, (20,))), 2)

    def test_logical_and_operator(self):
        """Test AND operator (&&)."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": True},
            {"age": 35, "active": False},
        ]
        expr = parse("$[?@.age > %d && @.active == %s]")

        results = expr.find(data, (27, True))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['age'], 30)

    def test_logical_or_operator(self):
        """Test OR operator (||)."""
        data = [
            {"age": 20},
            {"age": 30},
            {"age": 40},
        ]
        expr = parse("$[?@.age < %d || @.age > %d]")

        results = expr.find(data, (25, 35))
        self.assertEqual(len(results), 2)  # age 20 and 40

    def test_logical_not_operator(self):
        """Test NOT operator (!)."""
        data = [
            {"active": True},
            {"active": False},
        ]
        expr = parse("$[?!(@.active == %s)]")

        results = expr.find(data, (True,))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['active'], False)

    def test_wildcard_with_filter(self):
        """Test wildcard projection with filter."""
        data = {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ]
        }
        expr = parse("$.users[?@.age > %d]")

        results = expr.find(data, (27,))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['name'], "Alice")
        self.assertEqual(results[1]['name'], "Charlie")

    def test_finditer_method(self):
        """Test finditer() method."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        expr = parse("$[?@.age > %d]")

        results = list(expr.finditer(data, (27,)))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['age'], 30)
        self.assertEqual(results[1]['age'], 35)


class TestErrorHandling(unittest.TestCase):
    """Test error handling."""

    def test_missing_positional_parameter(self):
        """Test error when positional parameter is missing."""
        expr = parse("$[?@.age > %d]")
        data = [{"age": 30}]

        with self.assertRaises(ValueError):
            expr.find(data, ())  # No parameters provided

    def test_missing_named_parameter(self):
        """Test error when named parameter is missing."""
        expr = parse("$[?@.age > %(min_age)d]")
        data = [{"age": 30}]

        with self.assertRaises(ValueError):
            expr.find(data, {})  # No parameters provided


class TestParenthesesSupport(unittest.TestCase):
    """Test parentheses for grouping logical conditions."""

    def setUp(self):
        """Set up test data."""
        self.data = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 17, "active": False},
            {"name": "Charlie", "age": 65, "active": True},
            {"name": "Diana", "age": 45, "active": False},
        ]

    def test_parentheses_with_or(self):
        """Test parentheses grouping with OR: (age 25-50) OR not active."""
        expr = parse("$[?(@.age >= %d && @.age <= %d) || @.active == %s]")
        results = expr.find(self.data, (25, 50, False))

        names = [r['name'] for r in results]
        # Alice (30, in range), Bob (not active), Diana (not active)
        self.assertIn("Alice", names)
        self.assertIn("Bob", names)
        self.assertIn("Diana", names)

    def test_parentheses_with_and(self):
        """Test parentheses grouping with AND."""
        expr = parse("$[?(@.age < %d || @.age > %d) && @.active == %s]")
        results = expr.find(self.data, (20, 60, True))

        names = [r['name'] for r in results]
        # Charlie (65, active)
        self.assertIn("Charlie", names)
        self.assertEqual(len(names), 1)

    def test_nested_parentheses(self):
        """Test nested parentheses."""
        expr = parse("$[?((@.age >= %d && @.age <= %d) || @.age > %d)]")
        results = expr.find(self.data, (25, 35, 60))

        names = [r['name'] for r in results]
        # Alice (30, in 25-35), Charlie (65, > 60)
        self.assertIn("Alice", names)
        self.assertIn("Charlie", names)

    def test_parentheses_with_not(self):
        """Test parentheses with NOT operator."""
        expr = parse("$[?!(@.age < %d || @.age > %d)]")
        results = expr.find(self.data, (20, 60))

        names = [r['name'] for r in results]
        # Alice (30), Diana (45) - ages between 20 and 60
        self.assertIn("Alice", names)
        self.assertIn("Diana", names)
        self.assertEqual(len(names), 2)


class TestNestedPathsSupport(unittest.TestCase):
    """Test nested paths (accessing fields through relationships)."""

    def setUp(self):
        """Set up test data with nested structure."""
        self.data = {
            "users": [
                {
                    "name": "Alice",
                    "profile": {"level": 5, "verified": True},
                    "orders": [{"total": 150}, {"total": 50}],
                },
                {
                    "name": "Bob",
                    "profile": {"level": 2, "verified": False},
                    "orders": [{"total": 200}],
                },
                {
                    "name": "Charlie",
                    "profile": {"level": 8, "verified": True},
                    "orders": [],
                },
            ]
        }

    def test_nested_path_simple(self):
        """Test simple nested path: @.profile.level."""
        expr = parse("$.users[?@.profile.level > %d]")
        results = expr.find(self.data, (4,))

        names = [r['name'] for r in results]
        self.assertIn("Alice", names)  # level 5
        self.assertIn("Charlie", names)  # level 8
        self.assertEqual(len(names), 2)

    def test_nested_path_boolean(self):
        """Test nested path with boolean: @.profile.verified."""
        expr = parse("$.users[?@.profile.verified == %s]")
        results = expr.find(self.data, (True,))

        names = [r['name'] for r in results]
        self.assertIn("Alice", names)
        self.assertIn("Charlie", names)
        self.assertEqual(len(names), 2)

    def test_nested_path_with_index(self):
        """Test nested path with array index: @.orders[0].total."""
        expr = parse("$.users[?@.orders[0].total > %d]")
        results = expr.find(self.data, (100,))

        names = [r['name'] for r in results]
        self.assertIn("Alice", names)  # orders[0].total = 150
        self.assertIn("Bob", names)    # orders[0].total = 200

    def test_nested_path_combined_with_direct_field(self):
        """Test nested path combined with direct field access."""
        expr = parse("$.users[?@.profile.level > %d && @.name == %s]")
        results = expr.find(self.data, (4, "Alice"))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Alice")


class TestNestedWildcardsSupport(unittest.TestCase):
    """Test nested wildcards (filtering by child collection existence)."""

    def setUp(self):
        """Set up test data with nested collections."""
        self.data = {
            "users": [
                {
                    "name": "Alice",
                    "orders": [
                        {"total": 150, "status": "completed"},
                        {"total": 50, "status": "pending"},
                    ],
                },
                {
                    "name": "Bob",
                    "orders": [
                        {"total": 200, "status": "completed"},
                    ],
                },
                {
                    "name": "Charlie",
                    "orders": [],
                },
            ]
        }

    def test_nested_filter_existence(self):
        """Test nested filter as existence check: users with orders > 100."""
        expr = parse("$.users[?@.orders[?@.total > %d]]")
        results = expr.find(self.data, (100,))

        names = [r['name'] for r in results]
        self.assertIn("Alice", names)  # has order with total 150
        self.assertIn("Bob", names)    # has order with total 200
        self.assertNotIn("Charlie", names)  # no orders

    def test_nested_filter_with_string(self):
        """Test nested filter with string comparison."""
        expr = parse("$.users[?@.orders[?@.status == %s]]")
        results = expr.find(self.data, ("completed",))

        names = [r['name'] for r in results]
        self.assertIn("Alice", names)
        self.assertIn("Bob", names)
        self.assertNotIn("Charlie", names)

    def test_nested_filter_no_match(self):
        """Test nested filter with no matches."""
        expr = parse("$.users[?@.orders[?@.total > %d]]")
        results = expr.find(self.data, (1000,))

        self.assertEqual(len(results), 0)

    def test_nested_filter_combined_with_parentheses(self):
        """Test nested filter combined with parentheses."""
        expr = parse("$.users[?@.orders[?@.total > %d] && @.name != %s]")
        results = expr.find(self.data, (100, "Bob"))

        names = [r['name'] for r in results]
        self.assertEqual(names, ["Alice"])

    def test_direct_wildcard_filter(self):
        """Test direct filter on nested array elements."""
        expr = parse("$.users[*].orders[?@.total > %d]")
        results = expr.find(self.data, (100,))

        # Returns the matching orders, not users
        totals = [r['total'] for r in results]
        self.assertIn(150, totals)
        self.assertIn(200, totals)
        self.assertEqual(len(totals), 2)


if __name__ == "__main__":
    unittest.main()
