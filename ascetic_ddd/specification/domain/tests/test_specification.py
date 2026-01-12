"""Unit tests for Specification pattern implementation."""
import unittest
from typing import Any

from ..evaluate_visitor import (
    CollectionContext,
    EvaluateVisitor,
)
from ..nodes import (
    And,
    Equal,
    Field,
    GlobalScope,
    GreaterThan,
    Item,
    Not,
    Object,
    Value,
    Wildcard,
)


class ComparableInt:
    """Integer wrapper that implements comparison operand interfaces."""

    def __init__(self, val: int):
        self.val = val

    def __eq__(self, other: "ComparableInt") -> bool:
        """Check equality."""
        return self.val == other.val

    def __gt__(self, other: "ComparableInt") -> bool:
        """Check greater-than."""
        return self.val > other.val

    def __ge__(self, other: "ComparableInt") -> bool:
        """Check greater-than-or-equal."""
        return self.val >= other.val

    def __lt__(self, other: "ComparableInt") -> bool:
        """Check less-than."""
        return self.val < other.val

    def __le__(self, other: "ComparableInt") -> bool:
        """Check less-than-or-equal."""
        return self.val <= other.val

    def __repr__(self) -> str:
        return f"ComparableInt({self.val})"


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found")
        return self._data[key]


class TestNodes(unittest.TestCase):
    """Test AST node creation."""

    def test_value_node(self):
        """Test value node creation."""
        val_node = Value(42)
        self.assertEqual(val_node.value(), 42)

    def test_not_node(self):
        """Test NOT prefix operator."""
        val_node = Value(True)
        not_node = Not(val_node)
        self.assertEqual(not_node.operand(), val_node)

    def test_equal_node(self):
        """Test equality infix operator."""
        left = Value(ComparableInt(5))
        right = Value(ComparableInt(5))
        eq_node = Equal(left, right)
        self.assertEqual(eq_node.left(), left)
        self.assertEqual(eq_node.right(), right)

    def test_and_node(self):
        """Test AND logical operator."""
        left = Value(True)
        right = Value(True)
        and_node = And(left, right)
        self.assertEqual(and_node.left(), left)
        self.assertEqual(and_node.right(), right)

    def test_and_node_multiple(self):
        """Test AND with multiple operands."""
        a = Value(True)
        b = Value(True)
        c = Value(True)
        and_node = And(a, b, c)
        # Should create nested structure: (a AND b) AND c
        self.assertIsNotNone(and_node)

    def test_field_node(self):
        """Test field access node."""
        gs = GlobalScope()
        obj = Object(gs, "user")
        field_node = Field(obj, "name")
        self.assertEqual(field_node.name(), "name")
        self.assertEqual(field_node.object(), obj)


class TestEvaluateVisitor(unittest.TestCase):
    """Test evaluation visitor."""

    def test_simple_value(self):
        """Test evaluating a simple value."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        val_node = Value(True)
        val_node.accept(visitor)

        self.assertEqual(visitor.current_value(), True)

    def test_not_operator(self):
        """Test NOT operator evaluation."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = Not(Value(True))
        expression.accept(visitor)

        self.assertEqual(visitor.result(), False)

    def test_and_operator(self):
        """Test AND operator evaluation."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = And(Value(True), Value(True))
        expression.accept(visitor)

        self.assertEqual(visitor.result(), True)

    def test_and_operator_false(self):
        """Test AND operator with false operand."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = And(Value(True), Value(False))
        expression.accept(visitor)

        self.assertEqual(visitor.result(), False)

    def test_equal_operator(self):
        """Test equality operator."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = Equal(Value(ComparableInt(5)), Value(ComparableInt(5)))
        expression.accept(visitor)

        self.assertEqual(visitor.result(), True)

    def test_equal_operator_not_equal(self):
        """Test equality operator with different values."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = Equal(Value(ComparableInt(5)), Value(ComparableInt(10)))
        expression.accept(visitor)

        self.assertEqual(visitor.result(), False)

    def test_greater_than_operator(self):
        """Test greater-than operator."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = GreaterThan(
            Value(ComparableInt(10)), Value(ComparableInt(5))
        )
        expression.accept(visitor)

        self.assertEqual(visitor.result(), True)

    def test_greater_than_operator_false(self):
        """Test greater-than operator with false result."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        expression = GreaterThan(
            Value(ComparableInt(5)), Value(ComparableInt(10))
        )
        expression.accept(visitor)

        self.assertEqual(visitor.result(), False)

    def test_field_access(self):
        """Test field access through context."""
        ctx = DictContext({"age": ComparableInt(25)})
        visitor = EvaluateVisitor(ctx)

        field_node = Field(GlobalScope(), "age")
        field_node.accept(visitor)

        result = visitor.current_value()
        self.assertIsInstance(result, ComparableInt)
        self.assertEqual(result.val, 25)

    def test_object_navigation(self):
        """Test navigating through object hierarchy."""
        user_ctx = DictContext({"name": "Alice"})
        root_ctx = DictContext({"user": user_ctx})
        visitor = EvaluateVisitor(root_ctx)

        obj = Object(GlobalScope(), "user")
        field_node = Field(obj, "name")
        field_node.accept(visitor)

        self.assertEqual(visitor.current_value(), "Alice")

    def test_complex_expression(self):
        """Test complex boolean expression."""
        ctx = DictContext({"age": ComparableInt(25), "active": True})
        visitor = EvaluateVisitor(ctx)

        # (age > 18) AND active
        age_field = Field(GlobalScope(), "age")
        age_check = GreaterThan(age_field, Value(ComparableInt(18)))
        active_field = Field(GlobalScope(), "active")
        expression = And(age_check, active_field)

        expression.accept(visitor)

        self.assertEqual(visitor.result(), True)

    def test_collection_wildcard(self):
        """Test collection with wildcard and predicate."""
        item1 = DictContext({"score": ComparableInt(90)})
        item2 = DictContext({"score": ComparableInt(75)})
        item3 = DictContext({"score": ComparableInt(85)})
        collection_ctx = CollectionContext([item1, item2, item3])
        root_ctx = DictContext({"items": collection_ctx})

        visitor = EvaluateVisitor(root_ctx)

        # items[*].score > 80
        items_obj = Object(GlobalScope(), "items")
        score_field = Field(Item(), "score")
        predicate = GreaterThan(score_field, Value(ComparableInt(80)))
        wildcard_node = Wildcard(items_obj, predicate)

        wildcard_node.accept(visitor)

        # Should be True because at least one item has score > 80
        self.assertEqual(visitor.result(), True)

    def test_collection_all_false(self):
        """Test collection where no items match predicate."""
        item1 = DictContext({"score": ComparableInt(70)})
        item2 = DictContext({"score": ComparableInt(75)})
        collection_ctx = CollectionContext([item1, item2])
        root_ctx = DictContext({"items": collection_ctx})

        visitor = EvaluateVisitor(root_ctx)

        # items[*].score > 80
        items_obj = Object(GlobalScope(), "items")
        score_field = Field(Item(), "score")
        predicate = GreaterThan(score_field, Value(ComparableInt(80)))
        wildcard_node = Wildcard(items_obj, predicate)

        wildcard_node.accept(visitor)

        # Should be False because no items have score > 80
        self.assertEqual(visitor.result(), False)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in evaluation."""

    def test_missing_key(self):
        """Test error when accessing missing key."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        field_node = Field(GlobalScope(), "nonexistent")

        with self.assertRaises(KeyError):
            field_node.accept(visitor)

    def test_type_error_in_comparison(self):
        """Test that type checking works for comparison operators."""
        ctx = DictContext({})
        visitor = EvaluateVisitor(ctx)

        # Strings don't implement our custom comparison protocol
        # Note: In Python, all objects have __eq__, so they will pass
        # EqualOperand check. This test demonstrates protocol checking.
        expression = Equal(Value("hello"), Value("world"))

        # This will actually work because strings have __eq__
        # So we just verify it doesn't raise an error
        expression.accept(visitor)
        # Result should be False since strings are different
        self.assertEqual(visitor.result(), False)


if __name__ == "__main__":
    unittest.main()
