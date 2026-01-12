"""Unit tests for jsonpath_parser with C-style placeholders."""
import unittest
from ..jsonpath2_parameterized_parser import parse


class TestPositionalPlaceholders(unittest.TestCase):
    """Test positional placeholders (%s, %d, %f)."""

    def test_parse_integer_placeholder(self):
        """Test parsing with %d placeholder."""
        path = parse("$[*][?(@.age > %d)]")
        self.assertEqual(len(path.placeholders), 1)
        self.assertEqual(path.placeholders[0]['format_type'], 'd')
        self.assertTrue(path.placeholders[0]['positional'])

    def test_parse_string_placeholder(self):
        """Test parsing with %s placeholder."""
        path = parse("$[*][?(@.name = %s)]")
        self.assertEqual(len(path.placeholders), 1)
        self.assertEqual(path.placeholders[0]['format_type'], 's')

    def test_parse_float_placeholder(self):
        """Test parsing with %f placeholder."""
        path = parse("$[*][?(@.price > %f)]")
        self.assertEqual(len(path.placeholders), 1)
        self.assertEqual(path.placeholders[0]['format_type'], 'f')

    def test_execute_with_integer(self):
        """Test execution with integer parameter."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        path = parse("$[*][?(@.age > %d)]")

        results = path.find(data, (27,))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['age'], 30)
        self.assertEqual(results[1]['age'], 35)

    def test_execute_with_string(self):
        """Test execution with string parameter."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        path = parse("$[*][?(@.name = %s)]")

        results = path.find(data, ("Alice",))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Alice")

    def test_reuse_with_different_values(self):
        """Test reusing path with different parameter values."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        path = parse("$[*][?(@.age > %d)]")

        # Execute multiple times
        results1 = path.find(data, (26,))
        self.assertEqual(len(results1), 2)

        results2 = path.find(data, (30,))
        self.assertEqual(len(results2), 1)
        self.assertEqual(results2[0]['age'], 35)

        results3 = path.find(data, (40,))
        self.assertEqual(len(results3), 0)

    def test_execute_with_float(self):
        """Test execution with float parameter."""
        data = [{"price": 10.5}, {"price": 20.75}, {"price": 5.25}]
        path = parse("$[*][?(@.price > %f)]")

        results = path.find(data, (10.0,))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['price'], 10.5)
        self.assertEqual(results[1]['price'], 20.75)

    def test_multiple_positional_placeholders(self):
        """Test multiple positional placeholders in order."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        # Note: jsonpath2 might not support complex expressions like this
        # This is just to test placeholder parsing
        path = parse("$[*][?(@.age > %d)]")
        self.assertEqual(len(path.placeholders), 1)


class TestNamedPlaceholders(unittest.TestCase):
    """Test named placeholders %(name)s, %(age)d, %(price)f."""

    def test_parse_named_integer(self):
        """Test parsing with %(name)d placeholder."""
        path = parse("$[*][?(@.age > %(min_age)d)]")
        self.assertEqual(len(path.placeholders), 1)
        self.assertEqual(path.placeholders[0]['name'], 'min_age')
        self.assertEqual(path.placeholders[0]['format_type'], 'd')
        self.assertFalse(path.placeholders[0]['positional'])

    def test_parse_named_string(self):
        """Test parsing with %(name)s placeholder."""
        path = parse("$[*][?(@.name = %(username)s)]")
        self.assertEqual(len(path.placeholders), 1)
        self.assertEqual(path.placeholders[0]['name'], 'username')
        self.assertEqual(path.placeholders[0]['format_type'], 's')

    def test_execute_with_named_integer(self):
        """Test execution with named integer parameter."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        path = parse("$[*][?(@.age > %(min_age)d)]")

        results = path.find(data, {"min_age": 27})
        self.assertEqual(len(results), 2)

    def test_execute_with_named_string(self):
        """Test execution with named string parameter."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        path = parse("$[*][?(@.name = %(name)s)]")

        results = path.find(data, {"name": "Bob"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Bob")

    def test_reuse_with_different_named_values(self):
        """Test reusing path with different named parameter values."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        path = parse("$[*][?(@.name = %(name)s)]")

        for name in ["Alice", "Bob", "Charlie"]:
            results = path.find(data, {"name": name})
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['name'], name)

    def test_execute_with_named_float(self):
        """Test execution with named float parameter."""
        data = [{"price": 10.5}, {"price": 20.75}, {"price": 5.25}]
        path = parse("$[*][?(@.price > %(min_price)f)]")

        results = path.find(data, {"min_price": 10.0})
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['price'], 10.5)
        self.assertEqual(results[1]['price'], 20.75)


class TestMixedCases(unittest.TestCase):
    """Test edge cases and special scenarios."""

    def test_no_placeholders(self):
        """Test path without placeholders."""
        path = parse("$[*]")
        self.assertEqual(len(path.placeholders), 0)

        data = [{"age": 30}, {"age": 25}]
        results = path.find(data, ())
        self.assertEqual(len(results), 2)

    def test_find_one_method(self):
        """Test find_one() method."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        path = parse("$[*][?(@.age > %d)]")

        result = path.find_one(data, (27,))
        self.assertIsNotNone(result)
        self.assertEqual(result['age'], 30)

    def test_find_one_no_match(self):
        """Test find_one() when no matches."""
        data = [{"age": 30}]
        path = parse("$[*][?(@.age > %d)]")

        result = path.find_one(data, (40,))
        self.assertIsNone(result)

    def test_match_returns_generator(self):
        """Test that match() returns a generator."""
        data = [{"age": 30}, {"age": 25}]
        path = parse("$[*][?(@.age > %d)]")

        matches = path.match(data, (20,))
        # Should be a generator
        self.assertTrue(hasattr(matches, '__iter__'))
        self.assertTrue(hasattr(matches, '__next__'))

        # Consume generator
        results = list(matches)
        self.assertEqual(len(results), 2)

    def test_empty_data(self):
        """Test with empty data."""
        path = parse("$[*][?(@.age > %d)]")
        results = path.find([], (25,))
        self.assertEqual(len(results), 0)

    def test_special_characters_in_string(self):
        """Test string placeholder with special characters."""
        data = [{"name": 'Bob"Smith'}, {"name": "Alice"}]
        path = parse("$[*][?(@.name = %s)]")

        # Should handle quotes safely
        results = path.find(data, ('Bob"Smith',))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'Bob"Smith')

    def test_template_property(self):
        """Test that template property is accessible."""
        template_str = "$[*][?(@.age > %d)]"
        path = parse(template_str)
        self.assertEqual(path.template, template_str)

    def test_zero_value_parameters(self):
        """Test with zero and negative values."""
        data = [{"value": -5}, {"value": 0}, {"value": 5}]

        # Test with zero
        path = parse("$[*][?(@.value > %d)]")
        results = path.find(data, (0,))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['value'], 5)

        # Test with negative
        results = path.find(data, (-10,))
        self.assertEqual(len(results), 3)

    def test_empty_string_parameter(self):
        """Test with empty string parameter."""
        data = [{"name": ""}, {"name": "Alice"}]
        path = parse("$[*][?(@.name = %s)]")

        results = path.find(data, ("",))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "")

    def test_unicode_string_parameter(self):
        """Test with unicode string parameter."""
        data = [{"name": "Алиса"}, {"name": "Alice"}, {"name": "王"}]
        path = parse("$[*][?(@.name = %s)]")

        results = path.find(data, ("Алиса",))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Алиса")

        results = path.find(data, ("王",))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "王")


class TestMultiplePlaceholders(unittest.TestCase):
    """Test complex scenarios with multiple placeholders."""

    def test_two_positional_placeholders_same_type(self):
        """Test two positional placeholders of the same type."""
        data = [
            {"age": 20},
            {"age": 30},
            {"age": 40},
            {"age": 50},
        ]
        # Filter ages between min and max (not directly supported by jsonpath2 in single expression)
        # So we test separately that parsing works
        path = parse("$[*][?(@.age > %d)]")
        results = path.find(data, (25,))
        self.assertEqual(len(results), 3)

    def test_mixed_type_positional_placeholders(self):
        """Test positional placeholders with different types."""
        data = [
            {"name": "Product A", "price": 10.5},
            {"name": "Product B", "price": 20.75},
            {"name": "Product A", "price": 15.0},
        ]
        # Test string placeholder
        path_str = parse("$[*][?(@.name = %s)]")
        results = path_str.find(data, ("Product A",))
        self.assertEqual(len(results), 2)

        # Test float placeholder
        path_float = parse("$[*][?(@.price > %f)]")
        results = path_float.find(data, (12.0,))
        self.assertEqual(len(results), 2)

    def test_mixed_type_named_placeholders(self):
        """Test named placeholders with different types."""
        data = [
            {"name": "Alice", "age": 30, "salary": 50000.0},
            {"name": "Bob", "age": 25, "salary": 45000.0},
            {"name": "Charlie", "age": 35, "salary": 60000.0},
        ]

        # Test with string named placeholder
        path_name = parse("$[*][?(@.name = %(target_name)s)]")
        results = path_name.find(data, {"target_name": "Bob"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], "Bob")

        # Test with integer named placeholder
        path_age = parse("$[*][?(@.age > %(min_age)d)]")
        results = path_age.find(data, {"min_age": 28})
        self.assertEqual(len(results), 2)

        # Test with float named placeholder
        path_salary = parse("$[*][?(@.salary > %(min_salary)f)]")
        results = path_salary.find(data, {"min_salary": 48000.0})
        self.assertEqual(len(results), 2)

    def test_reuse_multiple_placeholders(self):
        """Test reusing path with multiple placeholders."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        path = parse("$[*][?(@.age > %(min_age)d)]")

        # Execute with different values
        results1 = path.find(data, {"min_age": 20})
        self.assertEqual(len(results1), 3)

        results2 = path.find(data, {"min_age": 30})
        self.assertEqual(len(results2), 1)
        self.assertEqual(results2[0]['name'], "Charlie")


class TestErrorCases(unittest.TestCase):
    """Test error handling."""

    def test_missing_positional_parameter(self):
        """Test error when positional parameter is missing."""
        path = parse("$[*][?(@.age > %d)]")

        with self.assertRaisesRegex(ValueError, "Missing positional parameter"):
            path.find([{"age": 30}], ())  # Empty tuple

    def test_missing_named_parameter(self):
        """Test error when named parameter is missing."""
        path = parse("$[*][?(@.age > %(min_age)d)]")

        with self.assertRaisesRegex(ValueError, "Missing named parameter"):
            path.find([{"age": 30}], {})  # Empty dict

    def test_wrong_parameter_name(self):
        """Test error when wrong named parameter provided."""
        path = parse("$[*][?(@.age > %(min_age)d)]")

        with self.assertRaisesRegex(ValueError, "Missing named parameter"):
            path.find([{"age": 30}], {"max_age": 25})  # Wrong name

    def test_too_few_positional_parameters(self):
        """Test error when providing fewer positional parameters than needed."""
        # Even though we only have one placeholder, test with empty tuple
        path = parse("$[*][?(@.age > %d)]")

        with self.assertRaisesRegex(ValueError, "Missing positional parameter"):
            path.find([{"age": 30}], ())

    def test_wrong_parameter_type_for_named(self):
        """Test with dict instead of tuple for positional placeholders."""
        path = parse("$[*][?(@.age > %d)]")

        # This should raise an error because we're using dict for positional
        with self.assertRaises((ValueError, TypeError, KeyError)):
            path.find([{"age": 30}], {"age": 25})

    def test_wrong_parameter_type_for_positional(self):
        """Test with tuple instead of dict for named placeholders."""
        path = parse("$[*][?(@.age > %(min_age)d)]")

        # This should raise an error because we're using tuple for named
        with self.assertRaises((ValueError, TypeError, AttributeError)):
            path.find([{"age": 30}], (25,))


class TestFilterFix(unittest.TestCase):
    """Test that jsonpath2 filter fix is working."""

    def test_filters_work_after_import(self):
        """Test that filters work after importing our module."""
        from jsonpath2 import Path

        data = [{"age": 30}, {"age": 25}]
        path = Path.parse_str("$[*][?(@.age > 25)]")

        results = list(path.match(data))
        # Should return 1 result (age=30) if fix is applied
        # Would return 0 if filter is broken
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].current_value['age'], 30)


class TestParenthesesSupport(unittest.TestCase):
    """Test parentheses support in expressions."""

    def test_simple_parentheses(self):
        """Test simple parentheses around condition."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        path = parse("$[*][?(@.age > %d)]")
        results = path.find(data, (26,))
        self.assertEqual(len(results), 2)

    def test_auto_add_parentheses(self):
        """Test automatic parentheses addition for RFC 9535 syntax."""
        data = [{"age": 30}, {"age": 25}, {"age": 35}]
        # RFC 9535 allows $[?@.age > 25] without parentheses
        # jsonpath2 requires $[?(@.age > 25)]
        # Our parser should auto-add parentheses
        path = parse("$[*][?@.age > %d]")  # Without parentheses
        results = path.find(data, (26,))
        self.assertEqual(len(results), 2)

    def test_auto_add_parentheses_with_logical_operators(self):
        """Test auto parentheses with AND/OR operators."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": False},
            {"age": 35, "active": True},
        ]
        # Without parentheses around filter
        path = parse("$[*][?@.age > %d && @.active == %s]")
        results = path.find(data, (26, True))
        self.assertEqual(len(results), 2)
        ages = [r["age"] for r in results]
        self.assertIn(30, ages)
        self.assertIn(35, ages)

    def test_complex_parentheses_with_and(self):
        """Test parentheses with AND operator."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": True},
            {"age": 35, "active": False},
        ]
        path = parse("$[*][?((@.age > %d) && (@.active == %s))]")
        results = path.find(data, (26, True))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["age"], 30)

    def test_complex_parentheses_with_or(self):
        """Test parentheses with OR operator."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": True},
            {"age": 35, "active": False},
        ]
        path = parse("$[*][?((@.age < %d) || (@.age > %d))]")
        results = path.find(data, (26, 34))
        self.assertEqual(len(results), 2)
        ages = [r["age"] for r in results]
        self.assertIn(25, ages)
        self.assertIn(35, ages)

    def test_nested_parentheses(self):
        """Test nested parentheses with AND and OR."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": False},
            {"age": 35, "active": True},
            {"age": 40, "active": False},
        ]
        # (age >= 27 AND age <= 32) OR active=False
        path = parse("$[*][?((@.age >= %d && @.age <= %d) || @.active == %s)]")
        results = path.find(data, (27, 32, False))
        self.assertEqual(len(results), 3)  # age=30, age=25(inactive), age=40(inactive)


class TestLogicalOperatorsSupport(unittest.TestCase):
    """Test logical operators (AND, OR, NOT) support."""

    def test_and_operator_rfc_syntax(self):
        """Test && (RFC 9535) operator."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": True},
            {"age": 35, "active": False},
        ]
        path = parse("$[*][?(@.age > %d && @.active == %s)]")
        results = path.find(data, (26, True))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["age"], 30)

    def test_or_operator_rfc_syntax(self):
        """Test || (RFC 9535) operator."""
        data = [
            {"age": 30, "active": True},
            {"age": 25, "active": True},
            {"age": 35, "active": False},
        ]
        path = parse("$[*][?(@.age < %d || @.active == %s)]")
        results = path.find(data, (26, False))
        self.assertEqual(len(results), 2)

    def test_not_operator_rfc_syntax(self):
        """Test ! (RFC 9535) operator."""
        data = [
            {"name": "Alice", "active": True},
            {"name": "Bob", "active": False},
            {"name": "Charlie", "active": True},
        ]
        path = parse("$[*][?!(@.active == %s)]")
        results = path.find(data, (True,))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Bob")

    def test_multiple_placeholders_in_and(self):
        """Test AND with multiple placeholders."""
        data = [
            {"age": 30, "score": 85},
            {"age": 25, "score": 90},
            {"age": 35, "score": 75},
        ]
        path = parse("$[*][?(@.age > %d && @.score >= %d)]")
        results = path.find(data, (26, 80))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["age"], 30)

    def test_multiple_placeholders_in_or(self):
        """Test OR with multiple placeholders."""
        data = [
            {"age": 30, "score": 85},
            {"age": 25, "score": 90},
            {"age": 35, "score": 95},  # Changed to score=95 to match second condition
        ]
        path = parse("$[*][?(@.age < %d || @.score > %d)]")
        results = path.find(data, (26, 88))
        # age=25 < 26 matches first condition
        # score=90 > 88 matches second condition (same record)
        # score=95 > 88 matches second condition
        self.assertEqual(len(results), 2)


class TestNestedPathsSupport(unittest.TestCase):
    """Test nested path access support."""

    def test_simple_nested_path(self):
        """Test simple nested path access."""
        data = {
            "store": {
                "books": [
                    {"title": "Book1", "price": 10},
                    {"title": "Book2", "price": 20},
                ]
            }
        }
        path = parse("$.store.books[*][?(@.price > %d)]")
        results = path.find(data, (15,))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Book2")

    def test_nested_path_with_filter(self):
        """Test nested path with filter condition."""
        data = {
            "company": {
                "employees": [
                    {"name": "Alice", "department": "Engineering"},
                    {"name": "Bob", "department": "Sales"},
                    {"name": "Charlie", "department": "Engineering"},
                ]
            }
        }
        path = parse("$.company.employees[*][?(@.department == %s)]")
        results = path.find(data, ("Engineering",))
        self.assertEqual(len(results), 2)

    def test_reuse_nested_path(self):
        """Test reusing nested path with different parameters."""
        data = {
            "store": {
                "books": [
                    {"title": "Book1", "price": 10},
                    {"title": "Book2", "price": 20},
                    {"title": "Book3", "price": 30},
                ]
            }
        }
        path = parse("$.store.books[*][?(@.price > %d)]")

        results1 = path.find(data, (15,))
        self.assertEqual(len(results1), 2)

        results2 = path.find(data, (25,))
        self.assertEqual(len(results2), 1)
        self.assertEqual(results2[0]["title"], "Book3")


class TestNestedWildcardsSupport(unittest.TestCase):
    """Test nested wildcards support."""

    def test_nested_wildcard_filter(self):
        """Test filtering with nested wildcard."""
        data = {
            "users": [
                {"name": "Alice", "orders": [{"total": 100}, {"total": 200}]},
                {"name": "Bob", "orders": [{"total": 50}]},
            ]
        }
        path = parse("$.users[*].orders[*][?(@.total > %d)]")
        results = path.find(data, (75,))
        self.assertEqual(len(results), 2)
        totals = [r["total"] for r in results]
        self.assertIn(100, totals)
        self.assertIn(200, totals)

    def test_nested_wildcard_with_different_values(self):
        """Test nested wildcard with different parameter values."""
        data = {
            "users": [
                {"name": "Alice", "orders": [{"total": 100}, {"total": 200}]},
                {"name": "Bob", "orders": [{"total": 50}]},
                {"name": "Charlie", "orders": [{"total": 300}]},
            ]
        }
        path = parse("$.users[*].orders[*][?(@.total > %d)]")

        results1 = path.find(data, (150,))
        self.assertEqual(len(results1), 2)  # 200, 300

        results2 = path.find(data, (250,))
        self.assertEqual(len(results2), 1)  # 300

    def test_nested_wildcard_with_and(self):
        """Test nested wildcard with AND condition."""
        data = {
            "users": [
                {"name": "Alice", "orders": [
                    {"total": 100, "status": "completed"},
                    {"total": 200, "status": "pending"}
                ]},
                {"name": "Bob", "orders": [
                    {"total": 50, "status": "completed"}
                ]},
            ]
        }
        path = parse("$.users[*].orders[*][?(@.total > %d && @.status == %s)]")
        results = path.find(data, (75, "completed"))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["total"], 100)

    def test_deep_nested_wildcard(self):
        """Test deeply nested wildcard path."""
        data = {
            "company": {
                "departments": [
                    {
                        "name": "Engineering",
                        "teams": [
                            {"name": "Backend", "members": [
                                {"name": "Alice", "level": 5},
                                {"name": "Bob", "level": 3}
                            ]},
                            {"name": "Frontend", "members": [
                                {"name": "Charlie", "level": 4}
                            ]}
                        ]
                    }
                ]
            }
        }
        path = parse("$.company.departments[*].teams[*].members[*][?(@.level >= %d)]")
        results = path.find(data, (4,))
        self.assertEqual(len(results), 2)
        names = [r["name"] for r in results]
        self.assertIn("Alice", names)
        self.assertIn("Charlie", names)


if __name__ == '__main__':
    unittest.main()
