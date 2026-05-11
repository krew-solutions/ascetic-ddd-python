"""Unit tests for infrastructure layer specification pattern."""
import unittest
from typing import Any, List, Protocol

from ascetic_ddd.specification.domain.nodes import (
    Equal,
    Not,
    Field,
    GlobalScope,
    Object,
    Value,
    And,
    Or,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    Sub,
    Item,
    Wildcard,
    IsNull,
    IsNotNull,
)
from ascetic_ddd.specification.infrastructure.schema import (
    SchemaRegistry,
    StorageType,
    ForeignKeyPair,
    CollectionMapping,
)

from ascetic_ddd.specification.infrastructure.composite_expression_node import CompositeExpressionsDifferentLengthError, CompositeExpression
from ascetic_ddd.specification.infrastructure.postgresql_visitor import (
    compile_specification,
    compile_to_sql,
    PostgresqlVisitor,
)
from ascetic_ddd.specification.infrastructure.transform_visitor import TransformVisitor
from ascetic_ddd.specification.infrastructure.transform_visitor import ITransformContext


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
        result = expr.accept(visitor)

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
        sql, params = expr.accept(visitor)

        self.assertEqual("users.name", sql)
        self.assertEqual([], params)

    def test_value_parameterization(self):
        """Test value rendering as parameterized placeholder."""

        expr = Value(42)

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

        self.assertEqual("$1", sql)
        self.assertEqual([42], params)

    def test_infix_operator_and(self):
        """Test AND operator rendering."""

        obj = Object(GlobalScope(), "t")
        # Create: a = 1 AND b = 2
        expr = And(Equal(Field(obj, "a"), Value(1)), Equal(Field(obj, "b"), Value(2)))

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

        self.assertIn("AND", sql)
        self.assertIn("t.a", sql)
        self.assertIn("t.b", sql)
        self.assertEqual([1, 2], params)

    def test_prefix_not_operator(self):
        """Test NOT prefix operator."""

        obj = Object(GlobalScope(), "t")
        expr = Not(Equal(Field(obj, "active"), Value(True)))

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

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


# =============================================================================
# IS NULL / IS NOT NULL Tests
# =============================================================================


class TestPostgresqlVisitorIsNull(unittest.TestCase):
    """Test IS NULL and IS NOT NULL operators."""

    def test_is_null(self):
        """Test IS NULL operator."""
        expr = IsNull(Field(GlobalScope(), "deleted_at"))

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

        self.assertEqual("deleted_at IS NULL", sql)
        self.assertEqual([], params)

    def test_is_not_null(self):
        """Test IS NOT NULL operator."""
        expr = IsNotNull(Field(GlobalScope(), "created_at"))

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

        self.assertEqual("created_at IS NOT NULL", sql)
        self.assertEqual([], params)

    def test_is_null_with_and(self):
        """Test IS NULL with AND operator."""
        expr = And(
            Equal(Field(GlobalScope(), "active"), Value(True)),
            IsNull(Field(GlobalScope(), "deleted_at")),
        )

        visitor = PostgresqlVisitor()
        sql, params = expr.accept(visitor)

        self.assertIn("IS NULL", sql)
        self.assertIn("AND", sql)
        self.assertEqual([True], params)


# =============================================================================
# compile_to_sql Tests
# =============================================================================


class TestCompileToSQL(unittest.TestCase):
    """Test compile_to_sql function."""

    def test_simple(self):
        """Simple expression: age >= 18"""
        expr = GreaterThanEqual(
            Field(GlobalScope(), "age"),
            Value(18),
        )

        sql, params = compile_to_sql(expr)

        self.assertEqual("age >= $1", sql)
        self.assertEqual([18], params)

    def test_complex(self):
        """Complex expression: (active = true) AND (age >= 18) OR (premium = true)"""
        expr = Or(
            And(
                Equal(Field(GlobalScope(), "active"), Value(True)),
                GreaterThanEqual(Field(GlobalScope(), "age"), Value(18)),
            ),
            Equal(Field(GlobalScope(), "premium"), Value(True)),
        )

        sql, params = compile_to_sql(expr)

        self.assertIn("AND", sql)
        self.assertIn("OR", sql)
        self.assertEqual(3, len(params))

    def test_with_wildcard(self):
        """Wildcard expression generates EXISTS with unnest."""
        expr = Wildcard(
            Object(GlobalScope(), "items"),
            GreaterThan(Field(Item(), "price"), Value(1000)),
        )

        sql, params = compile_to_sql(expr)

        self.assertIn("EXISTS", sql)
        self.assertIn("unnest", sql)
        self.assertEqual([1000], params)

    def test_nested_object(self):
        """Nested object: user.profile.age >= 18"""
        gs = GlobalScope()
        user = Object(gs, "user")
        profile = Object(user, "profile")

        expr = GreaterThanEqual(
            Field(profile, "age"),
            Value(18),
        )

        sql, params = compile_to_sql(expr)

        self.assertEqual("user.profile.age >= $1", sql)
        self.assertEqual([18], params)

    def test_arithmetic(self):
        """Arithmetic: (price - discount) > 100"""
        expr = GreaterThan(
            Sub(
                Field(GlobalScope(), "price"),
                Field(GlobalScope(), "discount"),
            ),
            Value(100),
        )

        sql, params = compile_to_sql(expr)

        self.assertIn("-", sql)
        self.assertIn("price", sql)
        self.assertIn("discount", sql)
        self.assertEqual([100], params)

    def test_negation(self):
        """Negation: NOT (age < 18)"""
        expr = Not(
            LessThan(
                Field(GlobalScope(), "age"),
                Value(18),
            ),
        )

        sql, params = compile_to_sql(expr)

        self.assertIn("NOT", sql)
        self.assertEqual([18], params)

    def test_is_null(self):
        """Test compile_to_sql with IS NULL."""
        expr = And(
            Equal(Field(GlobalScope(), "active"), Value(True)),
            IsNull(Field(GlobalScope(), "deleted_at")),
        )

        sql, params = compile_to_sql(expr)

        self.assertIn("IS NULL", sql)
        self.assertEqual(1, len(params))

    def test_with_schema(self):
        """Test compile_to_sql with schema for relational collections."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
        )

        expr = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        sql, params = compile_to_sql(expr, schema=schema)

        self.assertIn("EXISTS", sql)
        self.assertIn("items", sql)
        self.assertIn("store_id = s.id", sql)
        self.assertNotIn("unnest", sql)


# =============================================================================
# Wildcard/Collection Tests - Embedded (JSONB/array)
# =============================================================================


class TestPostgresqlVisitorWildcardEmbedded(unittest.TestCase):
    """Test PostgreSQL visitor wildcard functionality for embedded collections."""

    def test_wildcard_any(self):
        """spec.Any(store.Items, func(item Item) bool { return item.Price > 1000 })"""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_wildcard_all(self):
        """spec.All(store.Items, func(item Item) bool { return item.Active })"""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            Field(Item(), "Active"),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Active)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([], params)

    def test_wildcard_complex_predicate(self):
        """Complex predicate: item.Price > 1000 AND item.Active AND item.Stock > 0"""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            And(
                And(
                    GreaterThan(Field(Item(), "Price"), Value(1000)),
                    Field(Item(), "Active"),
                ),
                GreaterThan(Field(Item(), "Stock"), Value(0)),
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1 AND item_1.Active AND item_1.Stock > $2)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000, 0], params)

    def test_wildcard_with_root_condition(self):
        """store.Active AND spec.Any(store.Items, ...)"""
        ast = And(
            Field(GlobalScope(), "Active"),
            Wildcard(
                Object(GlobalScope(), "Items"),
                GreaterThan(Field(Item(), "Price"), Value(1000)),
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "Active AND EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_wildcard_negated(self):
        """NOT spec.Any(store.Items, ...)"""
        ast = Not(
            Wildcard(
                Object(GlobalScope(), "Items"),
                GreaterThan(Field(Item(), "Price"), Value(5000)),
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "NOT EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([5000], params)

    def test_wildcard_arithmetic(self):
        """item.Price - 100 > 900"""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(
                Sub(Field(Item(), "Price"), Value(100)),
                Value(900),
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price - $1 > $2)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([100, 900], params)

    def test_wildcard_multiple(self):
        """Multiple wildcards in same expression"""
        ast = And(
            And(
                Field(GlobalScope(), "Active"),
                Wildcard(
                    Object(GlobalScope(), "Items"),
                    GreaterThan(Field(Item(), "Price"), Value(1000)),
                ),
            ),
            Wildcard(
                Object(GlobalScope(), "Items"),
                LessThan(Field(Item(), "Price"), Value(100)),
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = (
            "Active AND EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1) "
            "AND EXISTS (SELECT 1 FROM unnest(Items) AS item_2 WHERE item_2.Price < $2)"
        )
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000, 100], params)

    def test_wildcard_less_than(self):
        """item.Price < 100"""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            LessThan(Field(Item(), "Price"), Value(100)),
        )

        visitor = PostgresqlVisitor()
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price < $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([100], params)


class TestPostgresqlVisitorWildcardNested(unittest.TestCase):
    """Test nested wildcard functionality."""

    def test_wildcard_nested(self):
        """Nested wildcard: store.Categories -> category.Items"""
        inner_wildcard = Wildcard(
            Object(Item(), "Items"),  # category.Items
            GreaterThan(Field(Item(), "Price"), Value(1000)),  # item.Price > 1000
        )

        outer_wildcard = Wildcard(
            Object(GlobalScope(), "Categories"),  # store.Categories
            inner_wildcard,
        )

        visitor = PostgresqlVisitor()
        sql, params = outer_wildcard.accept(visitor)

        expected_sql = (
            "EXISTS (SELECT 1 FROM unnest(Categories) AS category_1 WHERE "
            "EXISTS (SELECT 1 FROM unnest(category_1.Items) AS item_2 WHERE item_2.Price > $1))"
        )
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_wildcard_nested_with_condition(self):
        """Nested wildcard with additional condition: cat.Active AND ..."""
        inner_wildcard = Wildcard(
            Object(Item(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        outer_wildcard = Wildcard(
            Object(GlobalScope(), "Categories"),
            And(
                Field(Item(), "Active"),  # category.Active
                inner_wildcard,
            ),
        )

        visitor = PostgresqlVisitor()
        sql, params = outer_wildcard.accept(visitor)

        expected_sql = (
            "EXISTS (SELECT 1 FROM unnest(Categories) AS category_1 WHERE "
            "category_1.Active AND EXISTS (SELECT 1 FROM unnest(category_1.Items) AS item_2 WHERE item_2.Price > $1))"
        )
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_wildcard_double_nested(self):
        """Triple nesting: store.Regions -> region.Categories -> category.Items"""
        innermost_wildcard = Wildcard(
            Object(Item(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(5000)),
        )

        middle_wildcard = Wildcard(
            Object(Item(), "Categories"),
            innermost_wildcard,
        )

        outer_wildcard = Wildcard(
            Object(GlobalScope(), "Regions"),
            middle_wildcard,
        )

        visitor = PostgresqlVisitor()
        sql, params = outer_wildcard.accept(visitor)

        expected_sql = (
            "EXISTS (SELECT 1 FROM unnest(Regions) AS region_1 WHERE "
            "EXISTS (SELECT 1 FROM unnest(region_1.Categories) AS category_2 WHERE "
            "EXISTS (SELECT 1 FROM unnest(category_2.Items) AS item_3 WHERE item_3.Price > $1)))"
        )
        self.assertEqual(expected_sql, sql)
        self.assertEqual([5000], params)


# =============================================================================
# Wildcard/Collection Tests - Relational (separate tables)
# =============================================================================


class TestSchemaRegistry(unittest.TestCase):
    """Test SchemaRegistry functionality."""

    def test_relational_simple_fk(self):
        """Relational collection with simple FK."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_relational_composite_fk(self):
        """Relational collection with composite FK (tenant_id, store_id)."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational_composite(
                "Items",
                "items",
                [
                    ForeignKeyPair("tenant_id", "tenant_id"),
                    ForeignKeyPair("store_id", "id"),
                ],
            )
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.tenant_id = s.tenant_id AND item_1.store_id = s.id AND item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_relational_triple_composite_fk(self):
        """Relational collection with triple composite FK."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational_composite(
                "Items",
                "items",
                [
                    ForeignKeyPair("tenant_id", "tenant_id"),
                    ForeignKeyPair("region_id", "region_id"),
                    ForeignKeyPair("store_id", "id"),
                ],
            )
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            Equal(Field(Item(), "Active"), Value(True)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.tenant_id = s.tenant_id AND item_1.region_id = s.region_id AND item_1.store_id = s.id AND item_1.Active = $1)"
        self.assertEqual(expected_sql, sql)

    def test_embedded_collection(self):
        """Embedded collection uses unnest."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_embedded("Items")
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)

    def test_default_to_embedded(self):
        """Unknown collection defaults to embedded."""
        schema = SchemaRegistry("stores").with_parent_alias("s")

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)

    def test_no_schema(self):
        """No schema defaults to embedded (backwards compatibility)."""
        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor()  # No schema
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)"
        self.assertEqual(expected_sql, sql)

    def test_relational_with_complex_predicate(self):
        """Relational with AND predicate."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            And(
                GreaterThan(Field(Item(), "Price"), Value(1000)),
                Equal(Field(Item(), "Active"), Value(True)),
            ),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND item_1.Price > $1 AND item_1.Active = $2)"
        self.assertEqual(expected_sql, sql)
        self.assertEqual(2, len(params))

    def test_mixed_collections(self):
        """One embedded, one relational."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_embedded("Tags")
            .register_relational("Items", "items", "store_id", "id")
        )

        # Test relational
        ast1 = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(100)),
        )

        sql1, _ = ast1.accept(PostgresqlVisitor(schema=schema))

        self.assertEqual(
            "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND item_1.Price > $1)",
            sql1,
        )

        # Test embedded
        ast2 = Wildcard(
            Object(GlobalScope(), "Tags"),
            Equal(Field(Item(), "Name"), Value("sale")),
        )

        sql2, _ = ast2.accept(PostgresqlVisitor(schema=schema))

        self.assertEqual(
            "EXISTS (SELECT 1 FROM unnest(Tags) AS tag_1 WHERE tag_1.Name = $1)",
            sql2,
        )

    def test_nested_relational_collections(self):
        """Nested relational: stores -> categories -> items."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Categories", "categories", "store_id", "id")
            .register_relational("Items", "items", "category_id", "id")
        )

        ast = Wildcard(
            Object(GlobalScope(), "Categories"),
            Wildcard(
                Object(Item(), "Items"),
                GreaterThan(Field(Item(), "Price"), Value(1000)),
            ),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = (
            "EXISTS (SELECT 1 FROM categories AS category_1 WHERE category_1.store_id = s.id AND "
            "EXISTS (SELECT 1 FROM items AS item_2 WHERE item_2.category_id = category_1.id AND item_2.Price > $1))"
        )
        self.assertEqual(expected_sql, sql)
        self.assertEqual([1000], params)

    def test_nested_relational_with_composite_fk(self):
        """Nested relational with composite FK."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational_composite(
                "Categories",
                "categories",
                [
                    ForeignKeyPair("tenant_id", "tenant_id"),
                    ForeignKeyPair("store_id", "id"),
                ],
            )
            .register_relational_composite(
                "Items",
                "items",
                [
                    ForeignKeyPair("tenant_id", "tenant_id"),
                    ForeignKeyPair("category_id", "id"),
                ],
            )
        )

        ast = Wildcard(
            Object(GlobalScope(), "Categories"),
            Wildcard(
                Object(Item(), "Items"),
                Equal(Field(Item(), "Active"), Value(True)),
            ),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = (
            "EXISTS (SELECT 1 FROM categories AS category_1 WHERE "
            "category_1.tenant_id = s.tenant_id AND category_1.store_id = s.id AND "
            "EXISTS (SELECT 1 FROM items AS item_2 WHERE "
            "item_2.tenant_id = category_1.tenant_id AND item_2.category_id = category_1.id AND item_2.Active = $1))"
        )
        self.assertEqual(expected_sql, sql)

    def test_custom_alias(self):
        """Relational with custom alias."""
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register(
                "Items",
                CollectionMapping(
                    storage=StorageType.RELATIONAL,
                    table="store_items",
                    foreign_keys=[ForeignKeyPair("store_id", "id")],
                    alias="si",
                ),
            )
        )

        ast = Wildcard(
            Object(GlobalScope(), "Items"),
            GreaterThan(Field(Item(), "Price"), Value(1000)),
        )

        visitor = PostgresqlVisitor(schema=schema)
        sql, params = ast.accept(visitor)

        expected_sql = "EXISTS (SELECT 1 FROM store_items AS si_1 WHERE si_1.store_id = s.id AND si_1.Price > $1)"
        self.assertEqual(expected_sql, sql)


if __name__ == "__main__":
    unittest.main()
