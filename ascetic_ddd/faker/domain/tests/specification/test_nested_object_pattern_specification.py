import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider
from ascetic_ddd.faker.domain.specification.nested_object_pattern_specification import (
    NestedObjectPatternSpecification
)
from ascetic_ddd.faker.domain.values.empty import empty


# =============================================================================
# Test Fixtures
# =============================================================================

@dataclasses.dataclass(frozen=True)
class StatusId:
    value: str


@dataclasses.dataclass
class Status:
    id: StatusId
    name: str


@dataclasses.dataclass(frozen=True)
class UserId:
    value: int


@dataclasses.dataclass
class User:
    id: UserId
    status_id: StatusId
    name: str


@dataclasses.dataclass(frozen=True)
class CompanyId:
    value: str


@dataclasses.dataclass
class Company:
    id: CompanyId
    owner_id: UserId
    name: str


class MockRepository:
    """Mock async repository for testing."""

    def __init__(self, storage: dict = None):
        self._storage = storage or {}

    async def get(self, id_):
        key = self._extract_key(id_)
        return self._storage.get(key)

    def _extract_key(self, id_):
        if hasattr(id_, 'value'):
            return id_.value
        return id_

    def add(self, obj):
        if hasattr(obj, 'id'):
            key = self._extract_key(obj.id)
            self._storage[key] = obj


class MockReferenceProvider(IReferenceProvider):
    """Mock reference provider for testing."""

    def __init__(self, repository: MockRepository, aggregate_provider: typing.Any):
        self._repository = repository
        self._aggregate_provider = aggregate_provider
        self._input = empty
        self._output = empty
        self._provider_name = None

    @property
    def aggregate_provider(self):
        return self._aggregate_provider

    @aggregate_provider.setter
    def aggregate_provider(self, value):
        self._aggregate_provider = value

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        self._provider_name = value

    async def populate(self, session):
        pass

    def set(self, value):
        self._input = value

    def get(self):
        return self._output

    async def create(self, session):
        return self._output

    async def append(self, session, value):
        pass

    def reset(self):
        self._input = empty
        self._output = empty

    def is_complete(self):
        return self._output is not empty

    def is_transient(self):
        return self._input is empty

    def empty(self, shunt=None):
        return MockReferenceProvider(self._repository, self._aggregate_provider)

    def do_empty(self, clone, shunt):
        pass

    async def setup(self, session):
        pass

    async def cleanup(self, session):
        pass

    def attach(self, aspect, observer, id_=None):
        return lambda: None

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass


class MockAggregateProvider:
    """Mock aggregate provider for testing."""

    def __init__(self, providers: dict = None, output_exporter: typing.Callable = None):
        self._providers = providers or {}
        self._output_exporter = output_exporter or (lambda x: x)


# =============================================================================
# Tests for NestedObjectPatternSpecification - Basic
# =============================================================================

class NestedObjectPatternSpecificationBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for NestedObjectPatternSpecification."""

    async def test_is_satisfied_by_simple_pattern_without_provider(self):
        """Simple pattern matching without provider should work."""
        spec = NestedObjectPatternSpecification(
            {'status': 'active'},
            lambda obj: obj
        )
        self.assertTrue(await spec.is_satisfied_by({'status': 'active', 'name': 'test'}))
        self.assertFalse(await spec.is_satisfied_by({'status': 'inactive', 'name': 'test'}))

    async def test_is_satisfied_by_nested_pattern_without_provider(self):
        """Nested pattern without provider should use simple subset check."""
        spec = NestedObjectPatternSpecification(
            {'address': {'city': 'Moscow'}},
            lambda obj: obj
        )
        self.assertTrue(await spec.is_satisfied_by({'address': {'city': 'Moscow', 'street': 'Main'}}))
        self.assertFalse(await spec.is_satisfied_by({'address': {'city': 'London'}}))

    def test_hash_uses_object_pattern(self):
        """hash() should use _object_pattern."""
        spec = NestedObjectPatternSpecification(
            {'status': 'active'},
            lambda obj: obj
        )

        from ascetic_ddd.seedwork.domain.utils.data import hashable
        expected_hash = hash(hashable({'status': 'active'}))
        self.assertEqual(hash(spec), expected_hash)

    def test_hash_equality(self):
        """Specifications with same _object_pattern should have equal hash."""
        spec1 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        self.assertEqual(hash(spec1), hash(spec2))

    def test_hash_inequality(self):
        """Specifications with different _object_pattern should have different hash."""
        spec1 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = NestedObjectPatternSpecification({'status': 'inactive'}, lambda obj: obj)
        self.assertNotEqual(hash(spec1), hash(spec2))

    def test_eq_uses_object_pattern(self):
        """__eq__ should compare _object_pattern."""
        spec1 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        self.assertEqual(spec1, spec2)

    def test_eq_different_patterns(self):
        """Specifications with different _object_pattern should not be equal."""
        spec1 = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = NestedObjectPatternSpecification({'status': 'inactive'}, lambda obj: obj)
        self.assertNotEqual(spec1, spec2)

    def test_str_uses_object_pattern(self):
        """__str__ should use _object_pattern."""
        spec = NestedObjectPatternSpecification(
            {'status': 'active'},
            lambda obj: obj
        )

        from ascetic_ddd.seedwork.domain.utils.data import hashable
        expected_str = str(hashable({'status': 'active'}))
        self.assertEqual(str(spec), expected_str)

    def test_eq_with_non_specification(self):
        """__eq__ with non-NestedObjectPatternSpecification should return False."""
        spec = NestedObjectPatternSpecification({'status': 'active'}, lambda obj: obj)
        self.assertNotEqual(spec, {'status': 'active'})
        self.assertNotEqual(spec, "string")
        self.assertNotEqual(spec, 123)


# =============================================================================
# Tests for NestedObjectPatternSpecification - Nested Lookup
# =============================================================================

class NestedObjectPatternSpecificationNestedLookupTestCase(IsolatedAsyncioTestCase):
    """Tests for nested lookup in is_satisfied_by()."""

    def setUp(self):
        # Setup Status aggregate
        self.status_active = Status(StatusId("active"), "Active")
        self.status_inactive = Status(StatusId("inactive"), "Inactive")

        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)
        self.status_repo.add(self.status_inactive)

        self.status_provider = MockAggregateProvider(
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name}
        )

        # Setup User aggregate with reference to Status
        self.user_alice = User(UserId(1), StatusId("active"), "Alice")
        self.user_bob = User(UserId(2), StatusId("inactive"), "Bob")

        self.user_repo = MockRepository()
        self.user_repo.add(self.user_alice)
        self.user_repo.add(self.user_bob)

        self.status_ref_provider = MockReferenceProvider(
            self.status_repo, self.status_provider
        )

        self.user_provider = MockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name}
        )

    async def test_nested_lookup_matches(self):
        """Nested lookup should match when foreign object satisfies pattern."""
        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice has active status
        self.assertTrue(await spec.is_satisfied_by(self.user_alice))
        # Bob has inactive status
        self.assertFalse(await spec.is_satisfied_by(self.user_bob))

    async def test_nested_lookup_returns_false_when_fk_is_none(self):
        """Nested lookup should return False when fk_id is None."""
        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': None, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.user_alice))

    async def test_nested_lookup_returns_false_when_foreign_obj_not_found(self):
        """Nested lookup should return False when foreign object not found."""
        user_with_unknown_status = User(UserId(3), StatusId("unknown"), "Charlie")

        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(user_with_unknown_status))

    async def test_nested_lookup_with_non_reference_provider(self):
        """Non-reference provider should return True if fk_id is not None."""
        # name is not a ReferenceProvider, it's just a value
        user_provider = MockAggregateProvider(
            providers={'name': "not_a_reference_provider"},
            output_exporter=lambda u: {'id': u.id.value, 'name': u.name}
        )

        spec = NestedObjectPatternSpecification(
            {'name': {'nested': 'value'}},  # name is not ReferenceProvider
            lambda u: {'id': u.id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: user_provider
        )

        # Should return True because fk_id ('Alice') is not None
        self.assertTrue(await spec.is_satisfied_by(self.user_alice))

    async def test_simple_value_comparison_with_provider(self):
        """Simple value comparison should work alongside nested lookup."""
        spec = NestedObjectPatternSpecification(
            {'name': 'Alice', 'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice matches both name and status
        self.assertTrue(await spec.is_satisfied_by(self.user_alice))
        # Bob doesn't match name
        self.assertFalse(await spec.is_satisfied_by(self.user_bob))

    async def test_simple_value_mismatch_with_provider(self):
        """Simple value mismatch should return False early."""
        spec = NestedObjectPatternSpecification(
            {'name': 'NonExistent', 'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.user_alice))


# =============================================================================
# Tests for NestedObjectPatternSpecification - Deep Nesting
# =============================================================================

class NestedObjectPatternSpecificationDeepNestingTestCase(IsolatedAsyncioTestCase):
    """Tests for deeply nested lookup."""

    def setUp(self):
        # Setup Status aggregate
        self.status_active = Status(StatusId("active"), "Active")
        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)

        self.status_provider = MockAggregateProvider(
            providers={},
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name}
        )

        # Setup User aggregate with reference to Status
        self.user_alice = User(UserId(1), StatusId("active"), "Alice")
        self.user_repo = MockRepository()
        self.user_repo.add(self.user_alice)

        self.status_ref_provider = MockReferenceProvider(
            self.status_repo, self.status_provider
        )

        self.user_provider = MockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name}
        )

        # Setup Company aggregate with reference to User (owner)
        self.company = Company(CompanyId("acme"), UserId(1), "Acme Corp")
        self.company_repo = MockRepository()
        self.company_repo.add(self.company)

        self.owner_ref_provider = MockReferenceProvider(
            self.user_repo, self.user_provider
        )

        self.company_provider = MockAggregateProvider(
            providers={'owner_id': self.owner_ref_provider},
            output_exporter=lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name}
        )

    async def test_two_level_nested_lookup(self):
        """Two-level nested lookup should work."""
        # Company -> User (owner) -> Status
        spec = NestedObjectPatternSpecification(
            {'owner_id': {'status_id': {'name': 'Active'}}},
            lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name},
            aggregate_provider_accessor=lambda: self.company_provider
        )

        self.assertTrue(await spec.is_satisfied_by(self.company))

    async def test_two_level_nested_lookup_no_match(self):
        """Two-level nested lookup should return False when doesn't match."""
        spec = NestedObjectPatternSpecification(
            {'owner_id': {'status_id': {'name': 'Inactive'}}},
            lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name},
            aggregate_provider_accessor=lambda: self.company_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.company))


# =============================================================================
# Tests for NestedObjectPatternSpecification - Cache
# =============================================================================

class NestedObjectPatternSpecificationCacheTestCase(IsolatedAsyncioTestCase):
    """Tests for cache behavior."""

    def setUp(self):
        self.status_active = Status(StatusId("active"), "Active")
        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)

        self.status_provider = MockAggregateProvider(
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name}
        )

        self.user_alice = User(UserId(1), StatusId("active"), "Alice")

        self.status_ref_provider = MockReferenceProvider(
            self.status_repo, self.status_provider
        )

        self.user_provider = MockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name}
        )

    async def test_cache_stores_result(self):
        """Cache should store lookup result."""
        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertEqual(len(spec._nested_cache), 0)
        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

    async def test_cache_key_includes_provider_type(self):
        """Cache key should include type(aggregate_provider)."""
        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.is_satisfied_by(self.user_alice)

        cache_key = list(spec._nested_cache.keys())[0]
        self.assertEqual(cache_key[0], MockAggregateProvider)
        self.assertEqual(cache_key[1], 'status_id')
        self.assertEqual(cache_key[2], 'active')

    async def test_cache_hit_avoids_lookup(self):
        """Cache hit should avoid repository lookup."""
        call_count = [0]
        original_get = self.status_repo.get

        async def counting_get(id_):
            call_count[0] += 1
            return await original_get(id_)

        self.status_repo.get = counting_get

        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # First call - should hit repository
        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(call_count[0], 1)

        # Second call - should use cache
        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(call_count[0], 1)

    async def test_clear_cache(self):
        """clear_cache() should clear the cache."""
        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

        spec.clear_cache()
        self.assertEqual(len(spec._nested_cache), 0)

    async def test_different_provider_types_have_different_cache_keys(self):
        """Different provider types should have different cache keys."""

        class AnotherMockAggregateProvider(MockAggregateProvider):
            pass

        another_provider = AnotherMockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name}
        )

        provider_index = [0]
        providers = [self.user_provider, another_provider]

        def get_provider():
            return providers[provider_index[0]]

        spec = NestedObjectPatternSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=get_provider
        )

        # First provider
        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

        # Second provider (different type)
        provider_index[0] = 1
        await spec.is_satisfied_by(self.user_alice)
        self.assertEqual(len(spec._nested_cache), 2)


# =============================================================================
# Tests for NestedObjectPatternSpecification - Accept
# =============================================================================

class NestedObjectPatternSpecificationAcceptTestCase(IsolatedAsyncioTestCase):
    """Tests for accept() method."""

    def test_accept_passes_object_pattern(self):
        """accept() should pass _object_pattern to visitor."""
        received_pattern = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_pattern[0] = pattern

        original_pattern = {'status_id': {'name': 'Active'}}
        spec = NestedObjectPatternSpecification(
            original_pattern,
            lambda obj: obj
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        self.assertEqual(received_pattern[0], original_pattern)
        self.assertIs(received_pattern[0], original_pattern)

    def test_accept_passes_aggregate_provider_accessor(self):
        """accept() should pass aggregate_provider_accessor to visitor."""
        received_accessor = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_accessor[0] = accessor

        accessor = lambda: "test_provider"
        spec = NestedObjectPatternSpecification(
            {'status': 'active'},
            lambda obj: obj,
            aggregate_provider_accessor=accessor
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        self.assertIs(received_accessor[0], accessor)

    def test_accept_passes_none_when_no_accessor(self):
        """accept() should pass None when no accessor provided."""
        received_accessor = ["not_none"]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_accessor[0] = accessor

        spec = NestedObjectPatternSpecification(
            {'status': 'active'},
            lambda obj: obj,
            aggregate_provider_accessor=None
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        self.assertIsNone(received_accessor[0])
