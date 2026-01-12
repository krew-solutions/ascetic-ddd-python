"""Unit tests for infrastructure layer specification pattern."""
import unittest
from typing import Any, List, Protocol

from ...domain.nodes import (
    Equal,
    Not,
    Field,
    GlobalScope,
    Object,
    Value,
    And,
)

from ..composite_expression_node import CompositeExpressionsDifferentLengthError, CompositeExpression
from ..postgresql_visitor import compile_specification, PostgresqlVisitor
from ..transform_visitor import TransformVisitor
from ..transform_visitor import ITransformContext


# =============================================================================
# Test Domain Models - Composite Value Objects
# =============================================================================


class TenantId:
    """Tenant ID value object."""

    def __init__(self, val: int):
        self.value = val

    def __eq__(self, other):
        return isinstance(other, TenantId) and self.value == other.value


class InternalMemberId:
    """Internal member ID value object."""

    def __init__(self, val: int):
        self.value = val

    def __eq__(self, other):
        return isinstance(other, InternalMemberId) and self.value == other.value


class SomethingId:
    """Something ID value object."""

    def __init__(self, val: int):
        self.value = val

    def __eq__(self, other):
        return isinstance(other, SomethingId) and self.value == other.value


class MemberId:
    """Composite member ID (tenant + internal member)."""

    def __init__(self, tenant_id: TenantId, member_id: InternalMemberId):
        self.tenant_id = tenant_id
        self.member_id = member_id

    def decompose(self) -> List[Any]:
        """Decompose into constituent parts."""
        return [self.tenant_id, self.member_id]


class MemberSomethingId:
    """Composite ID for something belonging to a member."""

    def __init__(self, member_id: MemberId, something_id: SomethingId):
        self.member_id = member_id
        self.something_id = something_id

    def decompose(self) -> List[Any]:
        """Decompose into constituent parts."""
        return [self.member_id, self.something_id]


# =============================================================================
# Test Context Implementation
# =============================================================================


class SomethingScopeContext:
    """Context for 'something' object scope."""

    def attr_node(self, parent, path: List[str]):
        """Map domain fields to infrastructure fields."""
        if not path:
            raise ValueError("Empty path")

        if path[0] == "id":
            # Map domain "id" to composite of infrastructure fields
            return CompositeExpression(
                CompositeExpression(
                    Field(parent, "tenant_id"),
                    Field(parent, "member_id"),
                ),
                Field(parent, "something_id"),
            )
        else:
            raise ValueError(f'Unknown field: {path[0]}')


class TestGlobalScopeContext:
    """Global scope context for tests."""

    def __init__(self):
        self.something = SomethingScopeContext()

    def attr_node(self, path: List[str]):
        """Map domain object paths to infrastructure."""
        if not path:
            raise ValueError("Empty path")

        if path[0] == "something":
            obj = Object(GlobalScope(), "something")
            return self.something.attr_node(obj, path[1:])
        else:
            raise ValueError(f'Unknown object: {path[0]}')

    def value_node(self, val: Any):
        """Transform domain values to infrastructure values."""
        # Handle simple value objects
        if isinstance(val, TenantId):
            return Value(val.value)
        elif isinstance(val, InternalMemberId):
            return Value(val.value)
        elif isinstance(val, SomethingId):
            return Value(val.value)
        # Handle composite value objects
        elif isinstance(val, MemberId):
            parts = val.decompose()
            nodes = [self.value_node(part) for part in parts]
            return CompositeExpression(*nodes)
        elif isinstance(val, MemberSomethingId):
            parts = val.decompose()
            nodes = [self.value_node(part) for part in parts]
            return CompositeExpression(*nodes)
        else:
            raise ValueError(f'Cannot export value: {val}')


# =============================================================================
# Test Criteria and Specification
# =============================================================================


class SomethingCriteria:
    """Criteria builder for 'something' domain object."""

    def id(self):
        """Get ID field."""
        return Field(self._obj(), "id")

    def _obj(self):
        """Get object node."""
        return Object(GlobalScope(), "something")


class SomethingSpecification:
    """Specification for finding something by composite ID."""

    def __init__(
        self, tenant_id: TenantId, member_id: InternalMemberId, something_id: SomethingId
    ):
        self.composite_id = MemberSomethingId(
            MemberId(tenant_id, member_id), something_id
        )

    def expression(self):
        """Build specification expression."""
        something = SomethingCriteria()
        return Equal(something.id(), Value(self.composite_id))

    def evaluate(self):
        """Compile to SQL."""
        context = TestGlobalScopeContext()
        return compile_specification(context, self.expression())


# =============================================================================
# Unit Tests
# =============================================================================


class TestCompositeExpressionNode(unittest.TestCase):
    """Test composite expression node functionality."""

    def test_equal_composite_simple(self):
        """Test equality between simple composite expressions."""
        left = CompositeExpression(Value(1), Value(2))
        right = CompositeExpression(Value(3), Value(4))

        result = left == right

        # Should create: (1 = 3) AND (2 = 4)
        self.assertIsNotNone(result)

    def test_equal_composite_different_length(self):
        """Test error when composite expressions have different lengths."""
        left = CompositeExpression(Value(1), Value(2))
        right = CompositeExpression(Value(3))

        with self.assertRaises(CompositeExpressionsDifferentLengthError):
            left == right

    def test_not_equal_composite_simple(self):
        """Test inequality between simple composite expressions."""
        left = CompositeExpression(Value(1), Value(2))
        right = CompositeExpression(Value(3), Value(4))

        result = left != right

        # Should create: NOT((1 = 3) AND (2 = 4))
        self.assertIsNotNone(result)

    def test_not_equal_composite_different_length(self):
        """Test error when composite expressions have different lengths."""
        left = CompositeExpression(Value(1), Value(2))
        right = CompositeExpression(Value(3))

        with self.assertRaises(CompositeExpressionsDifferentLengthError):
            left != right

    def test_nested_composite_equal(self):
        """Test equality with nested composite expressions."""
        # Create nested structure: ((a, b), c)
        left = CompositeExpression(
            CompositeExpression(Value(1), Value(2)), Value(3)
        )
        right = CompositeExpression(
            CompositeExpression(Value(4), Value(5)), Value(6)
        )

        result = left == right

        # Should create: ((1 = 4) AND (2 = 5)) AND (3 = 6)
        self.assertIsNotNone(result)

    def test_nested_composite_type_mismatch(self):
        """Test error when nested structure doesn't match."""
        # Left is nested, right is flat
        left = CompositeExpression(CompositeExpression(Value(1), Value(2)), Value(3))
        right = CompositeExpression(Value(4), Value(5))

        with self.assertRaises(CompositeExpressionsDifferentLengthError):
            left == right


class TestTransformVisitor(unittest.TestCase):
    """Test transform visitor functionality."""

    def test_field_transformation(self):
        """Test field path transformation."""
        something = SomethingCriteria()
        expr = Equal(
            something.id(),
            Value(
                MemberSomethingId(
                    MemberId(TenantId(10), InternalMemberId(3)), SomethingId(5)
                )
            ),
        )

        context = TestGlobalScopeContext()

        visitor = TransformVisitor(context)
        expr.accept(visitor)
        result = visitor.result()

        self.assertIsNotNone(result)

    def test_value_decomposition(self):
        """Test value object decomposition."""
        composite_id = MemberSomethingId(
            MemberId(TenantId(10), InternalMemberId(3)), SomethingId(5)
        )

        context = TestGlobalScopeContext()
        result = context.value_node(composite_id)

        self.assertIsInstance(result, CompositeExpression)


class TestPostgresqlVisitor(unittest.TestCase):
    """Test PostgreSQL visitor functionality."""

    def test_simple_field_rendering(self):
        """Test rendering of simple field path."""

        obj = Object(GlobalScope(), "users")
        expr = Field(obj, "name")

        visitor = PostgresqlVisitor()
        expr.accept(visitor)
        sql, params = visitor.result()

        self.assertEqual("users.name", sql)
        self.assertEqual([], params)

    def test_value_parameterization(self):
        """Test value rendering as parameterized placeholder."""

        expr = Value(42)

        visitor = PostgresqlVisitor()
        expr.accept(visitor)
        sql, params = visitor.result()

        self.assertEqual("$1", sql)
        self.assertEqual([42], params)

    def test_infix_operator_and(self):
        """Test AND operator rendering."""

        obj = Object(GlobalScope(), "t")
        # Create: a = 1 AND b = 2
        expr = And(Equal(Field(obj, "a"), Value(1)), Equal(Field(obj, "b"), Value(2)))

        visitor = PostgresqlVisitor()
        expr.accept(visitor)
        sql, params = visitor.result()

        self.assertIn("AND", sql)
        self.assertIn("t.a", sql)
        self.assertIn("t.b", sql)
        self.assertEqual([1, 2], params)

    def test_prefix_not_operator(self):
        """Test NOT prefix operator."""

        obj = Object(GlobalScope(), "t")
        expr = Not(Equal(Field(obj, "active"), Value(True)))

        visitor = PostgresqlVisitor()
        expr.accept(visitor)
        sql, params = visitor.result()

        self.assertIn("NOT", sql)
        self.assertIn("t.active", sql)
        self.assertEqual([True], params)


class TestEndToEnd(unittest.TestCase):
    """End-to-end integration tests."""

    def test_composite_key_specification(self):
        """Test complete composite key specification compilation."""
        # Create specification with composite ID
        spec = SomethingSpecification(
            tenant_id=TenantId(10),
            member_id=InternalMemberId(3),
            something_id=SomethingId(5),
        )

        # Compile to SQL
        sql, params = spec.evaluate()

        # Expected: something.tenant_id = $1 AND something.member_id = $2 AND something.something_id = $3
        self.assertEqual(
            "something.tenant_id = $1 AND something.member_id = $2 AND something.something_id = $3",
            sql,
        )
        self.assertEqual([10, 3, 5], params)

    def test_multiple_specifications(self):
        """Test multiple specifications with different IDs."""
        spec1 = SomethingSpecification(
            TenantId(1), InternalMemberId(2), SomethingId(3)
        )
        spec2 = SomethingSpecification(
            TenantId(10), InternalMemberId(20), SomethingId(30)
        )

        sql1, params1 = spec1.evaluate()
        sql2, params2 = spec2.evaluate()

        # SQL should be the same
        self.assertEqual(sql1, sql2)

        # But parameters should differ
        self.assertEqual([1, 2, 3], params1)
        self.assertEqual([10, 20, 30], params2)


if __name__ == "__main__":
    unittest.main()
