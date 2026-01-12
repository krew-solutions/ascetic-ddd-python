"""Unit tests for lambda function parser."""
import unittest
from typing import Any

from ...lambda_filter.lambda_parser import parse
from ...nodes import (
    Add,
    And,
    Div,
    Equal,
    Field,
    GlobalScope,
    GreaterThan,
    GreaterThanEqual,
    Item,
    LessThan,
    LessThanEqual,
    Mod,
    Mul,
    Not,
    NotEqual,
    Or,
    Sub,
    Value,
    Wildcard,
    Object,
)
from ...evaluate_visitor import EvaluateVisitor


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

    def __iter__(self):
        return iter(self._items)

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


class TestLambdaParser(unittest.TestCase):
    """Test lambda function parser."""

    def test_simple_greater_than(self):
        """Test simple greater-than comparison."""
        spec = parse(lambda user: user.age > 25)

        # Verify AST structure
        self.assertIsInstance(spec, GreaterThan)
        self.assertIsInstance(spec.left(), Field)
        self.assertEqual(spec.left().name(), "age")
        self.assertIsInstance(spec.right(), Value)
        self.assertEqual(spec.right().value(), 25)

        # Verify evaluation
        user = DictContext({"age": 30})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 20})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_simple_less_than(self):
        """Test simple less-than comparison."""
        spec = parse(lambda user: user.age < 30)

        user = DictContext({"age": 25})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 35})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_simple_equal(self):
        """Test simple equality comparison."""
        spec = parse(lambda user: user.name == "Alice")

        self.assertIsInstance(spec, Equal)

        user = DictContext({"name": "Alice"})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"name": "Bob"})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_simple_not_equal(self):
        """Test simple not-equal comparison."""
        spec = parse(lambda user: user.status != "deleted")

        user = DictContext({"status": "active"})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"status": "deleted"})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_greater_than_or_equal(self):
        """Test greater-than-or-equal comparison."""
        spec = parse(lambda user: user.age >= 30)

        user = DictContext({"age": 30})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 35})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 25})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_less_than_or_equal(self):
        """Test less-than-or-equal comparison."""
        spec = parse(lambda user: user.age <= 30)

        user = DictContext({"age": 30})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 25})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 35})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_logical_and(self):
        """Test logical AND operator."""
        spec = parse(lambda user: user.age > 25 and user.active == True)

        self.assertIsInstance(spec, And)

        user = DictContext({"age": 30, "active": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 30, "active": False})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

        user = DictContext({"age": 20, "active": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_logical_or(self):
        """Test logical OR operator."""
        spec = parse(lambda user: user.age < 18 or user.age > 65)

        self.assertIsInstance(spec, Or)

        user = DictContext({"age": 15})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 70})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 30})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_logical_not(self):
        """Test logical NOT operator."""
        spec = parse(lambda user: not user.deleted)

        self.assertIsInstance(spec, Not)

        user = DictContext({"deleted": False})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"deleted": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_complex_expression(self):
        """Test complex expression with multiple operators."""
        spec = parse(lambda user: user.age >= 18 and user.age <= 65 and user.active == True)

        user = DictContext({"age": 30, "active": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        user = DictContext({"age": 30, "active": False})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

        user = DictContext({"age": 70, "active": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_any_with_generator_expression(self):
        """Test any() with generator expression -> Wildcard."""
        spec = parse(lambda store: any(item.price > 500 for item in store.items))

        self.assertIsInstance(spec, Wildcard)
        self.assertEqual(spec.name(), "*")

        # Check predicate structure
        predicate = spec.predicate()
        self.assertIsInstance(predicate, GreaterThan)

        # Test evaluation
        item1 = DictContext({"name": "Laptop", "price": 999})
        item2 = DictContext({"name": "Mouse", "price": 29})

        items = CollectionContext([item1, item2])
        store = DictContext({"items": items})

        visitor = EvaluateVisitor(store)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # Laptop price > 500

        # Test with all items below threshold
        item1 = DictContext({"name": "Mouse", "price": 29})
        item2 = DictContext({"name": "Keyboard", "price": 49})

        items = CollectionContext([item1, item2])
        store = DictContext({"items": items})

        visitor = EvaluateVisitor(store)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_any_with_list_comprehension(self):
        """Test any() with list comprehension -> Wildcard."""
        spec = parse(lambda store: any([item.price > 500 for item in store.items]))

        self.assertIsInstance(spec, Wildcard)

        item1 = DictContext({"name": "Laptop", "price": 999})
        item2 = DictContext({"name": "Mouse", "price": 29})

        items = CollectionContext([item1, item2])
        store = DictContext({"items": items})

        visitor = EvaluateVisitor(store)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_wildcard_with_complex_predicate(self):
        """Test wildcard with complex predicate."""
        spec = parse(lambda store: any(item.price > 100 and item.available == True for item in store.items))

        self.assertIsInstance(spec, Wildcard)
        self.assertIsInstance(spec.predicate(), And)

        item1 = DictContext({"name": "Laptop", "price": 999, "available": True})
        item2 = DictContext({"name": "Mouse", "price": 29, "available": True})

        items = CollectionContext([item1, item2])
        store = DictContext({"items": items})

        visitor = EvaluateVisitor(store)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        # Test with high price but not available
        item1 = DictContext({"name": "Laptop", "price": 999, "available": False})
        item2 = DictContext({"name": "Mouse", "price": 29, "available": True})

        items = CollectionContext([item1, item2])
        store = DictContext({"items": items})

        visitor = EvaluateVisitor(store)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_any_list_comprehension(self):
        """Test nested any with list comprehensions - Wildcard inside Wildcard."""
        spec = parse(lambda order: any([
            any([item.price > 100 for item in category.items])
            for category in order.categories
        ]))

        # Verify AST structure - outer Wildcard
        self.assertIsInstance(spec, Wildcard)

        # Inner predicate should also be Wildcard
        outer_predicate = spec.predicate()
        self.assertIsInstance(outer_predicate, Wildcard)

        # Create test data structure:
        # order
        #   └── categories (collection)
        #         ├── category1
        #         │     └── items (collection)
        #         │           ├── item1 (price: 150)
        #         │           └── item2 (price: 50)
        #         └── category2
        #               └── items (collection)
        #                     ├── item3 (price: 80)
        #                     └── item4 (price: 30)

        # Category 1 has expensive item
        item1 = DictContext({"name": "Laptop", "price": 150})
        item2 = DictContext({"name": "Mouse", "price": 50})
        items1 = CollectionContext([item1, item2])
        category1 = DictContext({"name": "Electronics", "items": items1})

        # Category 2 has only cheap items
        item3 = DictContext({"name": "Pen", "price": 80})
        item4 = DictContext({"name": "Notebook", "price": 30})
        items2 = CollectionContext([item3, item4])
        category2 = DictContext({"name": "Stationery", "items": items2})

        categories = CollectionContext([category1, category2])
        order = DictContext({"id": 1, "categories": categories})

        # Test: should be True because category1 has item with price > 100
        visitor = EvaluateVisitor(order)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        # Test with all cheap items
        item1_cheap = DictContext({"name": "Laptop", "price": 90})
        item2_cheap = DictContext({"name": "Mouse", "price": 50})
        items1_cheap = CollectionContext([item1_cheap, item2_cheap])
        category1_cheap = DictContext({"name": "Electronics", "items": items1_cheap})

        item3_cheap = DictContext({"name": "Pen", "price": 80})
        item4_cheap = DictContext({"name": "Notebook", "price": 30})
        items2_cheap = CollectionContext([item3_cheap, item4_cheap])
        category2_cheap = DictContext({"name": "Stationery", "items": items2_cheap})

        categories_cheap = CollectionContext([category1_cheap, category2_cheap])
        order_cheap = DictContext({"id": 2, "categories": categories_cheap})

        # Test: should be False because no items have price > 100
        visitor = EvaluateVisitor(order_cheap)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_any_generator_expression(self):
        """Test nested any with generator expressions."""
        spec = parse(lambda order: any(
            any(item.price > 100 for item in category.items)
            for category in order.categories
        ))

        # Verify AST structure
        self.assertIsInstance(spec, Wildcard)
        self.assertIsInstance(spec.predicate(), Wildcard)

        # Create test data
        item1 = DictContext({"name": "Premium", "price": 150})
        item2 = DictContext({"name": "Standard", "price": 50})
        items = CollectionContext([item1, item2])
        category = DictContext({"name": "Products", "items": items})

        categories = CollectionContext([category])
        order = DictContext({"id": 1, "categories": categories})

        visitor = EvaluateVisitor(order)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_boolean_literal_true(self):
        """Test boolean literal True."""
        spec = parse(lambda user: user.active == True)

        user = DictContext({"active": True})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_boolean_literal_false(self):
        """Test boolean literal False."""
        spec = parse(lambda user: user.active == False)

        user = DictContext({"active": False})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_string_literal(self):
        """Test string literal."""
        spec = parse(lambda user: user.role == "admin")

        user = DictContext({"role": "admin"})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_integer_literal(self):
        """Test integer literal."""
        spec = parse(lambda user: user.count == 42)

        user = DictContext({"count": 42})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_float_literal(self):
        """Test float literal."""
        spec = parse(lambda product: product.price > 99.99)

        product = DictContext({"price": 149.99})
        visitor = EvaluateVisitor(product)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

    def test_arithmetic_addition(self):
        """Test arithmetic addition."""
        spec = parse(lambda user: user.age + 5 > 30)

        self.assertIsInstance(spec, GreaterThan)
        self.assertIsInstance(spec.left(), Add)

        user = DictContext({"age": 26})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # 26 + 5 = 31 > 30

        user = DictContext({"age": 24})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # 24 + 5 = 29 < 30

    def test_arithmetic_subtraction(self):
        """Test arithmetic subtraction."""
        spec = parse(lambda user: user.age - 5 >= 18)

        user = DictContext({"age": 25})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # 25 - 5 = 20 >= 18

        user = DictContext({"age": 20})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # 20 - 5 = 15 < 18

    def test_arithmetic_multiplication(self):
        """Test arithmetic multiplication."""
        spec = parse(lambda product: product.price * 2 > 100)

        product = DictContext({"price": 60})
        visitor = EvaluateVisitor(product)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # 60 * 2 = 120 > 100

        product = DictContext({"price": 40})
        visitor = EvaluateVisitor(product)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # 40 * 2 = 80 < 100

    def test_arithmetic_division(self):
        """Test arithmetic division."""
        spec = parse(lambda user: user.score / 2 >= 40)

        user = DictContext({"score": 85})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # 85 / 2 = 42.5 >= 40

        user = DictContext({"score": 70})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # 70 / 2 = 35 < 40

    def test_arithmetic_modulo(self):
        """Test arithmetic modulo."""
        spec = parse(lambda user: user.id % 2 == 0)

        user = DictContext({"id": 10})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # 10 % 2 = 0

        user = DictContext({"id": 11})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # 11 % 2 = 1

    def test_complex_arithmetic(self):
        """Test complex arithmetic expression."""
        spec = parse(lambda user: (user.age + 5) * 2 > 60)

        user = DictContext({"age": 28})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertTrue(visitor.result())  # (28 + 5) * 2 = 66 > 60

        user = DictContext({"age": 25})
        visitor = EvaluateVisitor(user)
        spec.accept(visitor)
        self.assertFalse(visitor.result())  # (25 + 5) * 2 = 60 == 60

    def test_nested_path_simple(self):
        """Test simple nested path: user.profile.age."""
        spec = parse(lambda user: user.profile.age > 25)

        # Verify AST structure
        self.assertIsInstance(spec, GreaterThan)
        self.assertIsInstance(spec.left(), Field)

        # Test with age > 25
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "name": "Alice"
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        # Test with age <= 25
        data = NestedDictContext({
            "profile": {
                "age": 20,
                "name": "Bob"
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_path_deep(self):
        """Test deep nested path: user.company.department.manager.level."""
        spec = parse(lambda user: user.company.department.manager.level > 5)

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

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

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

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_path_with_and_operator(self):
        """Test nested path with AND operator."""
        spec = parse(lambda user: user.profile.age > 25 and user.profile.active == True)

        self.assertIsInstance(spec, And)

        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": True
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        # Test with active = False
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": False
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_path_with_or_operator(self):
        """Test nested path with OR operator."""
        spec = parse(lambda user: user.profile.age < 18 or user.profile.age > 65)

        data = NestedDictContext({
            "profile": {
                "age": 15
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        data = NestedDictContext({
            "profile": {
                "age": 70
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        data = NestedDictContext({
            "profile": {
                "age": 30
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_path_equality(self):
        """Test nested path with equality comparison."""
        spec = parse(lambda user: user.profile.status == "active")

        data = NestedDictContext({
            "profile": {
                "status": "active"
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        data = NestedDictContext({
            "profile": {
                "status": "inactive"
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

    def test_nested_path_complex_expression(self):
        """Test nested path with complex expression."""
        spec = parse(lambda user: user.profile.age >= 18 and user.profile.age <= 65 and user.profile.active == True)

        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": True
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertTrue(visitor.result())

        # Test with age out of range
        data = NestedDictContext({
            "profile": {
                "age": 70,
                "active": True
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())

        # Test with active = False
        data = NestedDictContext({
            "profile": {
                "age": 30,
                "active": False
            }
        })

        visitor = EvaluateVisitor(data)
        spec.accept(visitor)
        self.assertFalse(visitor.result())


if __name__ == "__main__":
    unittest.main()
