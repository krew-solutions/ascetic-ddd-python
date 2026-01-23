import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.object_pattern_lookup_specification import (
    ObjectPatternLookupSpecification
)
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.faker.infrastructure.repositories.in_memory_repository import InMemoryRepository


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


class MockSession:
    """Mock session for testing."""
    pass


class StubDistributor(IM2ODistributor):
    """Stub distributor that returns predefined values."""

    def __init__(self, values: list = None, raise_cursor: bool = False):
        self._values = values or []
        self._index = 0
        self._raise_cursor = raise_cursor
        self._appended = []
        self._provider_name = None
        self._observers = []

    async def next(self, session: ISession, specification: ISpecification = None):
        if self._raise_cursor or self._index >= len(self._values):
            raise Cursor(position=self._index, callback=self._append)
        value = self._values[self._index]
        self._index += 1
        return value

    async def _append(self, session: ISession, value, position):
        self._appended.append(value)

    async def append(self, session: ISession, value):
        self._appended.append(value)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        self._provider_name = value

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def bind_external_source(self, external_source: typing.Any) -> None:
        pass

    def attach(self, aspect, observer, id_=None):
        self._observers.append((aspect, observer))
        return lambda: self._observers.remove((aspect, observer))

    def detach(self, aspect, observer, id_=None):
        self._observers = [(a, o) for a, o in self._observers if o != observer]

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self




class MockRepository:
    """Mock async repository for testing."""

    def __init__(self, storage: dict = None):
        self._storage = storage or {}

    async def get(self, session, id_):
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

    def __init__(
            self,
            providers: dict = None,
            output_exporter: typing.Callable = None,
            repository: typing.Any = None
    ):
        self._providers = providers or {}
        self._output_exporter = output_exporter or (lambda x: x)
        self._repository = repository


# =============================================================================
# Real Providers for Sociable Tests
# =============================================================================

class StatusFaker(AggregateProvider[dict, Status]):
    """Real StatusFaker provider for sociable tests."""
    _id_attr = 'id'

    id: ValueProvider[str, StatusId]
    name: ValueProvider[str, str]

    def __init__(self, repository: IAggregateRepository, distributor: IM2ODistributor):
        self.id = ValueProvider(
            distributor=distributor,
            input_generator=lambda session, pos=None: "status_%s" % (pos or 0),
            output_factory=StatusId,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(values=["Active", "Inactive", "Pending"]),
            input_generator=lambda session, pos=None: "Status %s" % (pos or 0),
        )
        super().__init__(
            repository=repository,
            output_factory=Status,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(status: Status) -> dict:
        return {
            'id': status.id.value if hasattr(status.id, 'value') else status.id,
            'name': status.name,
        }


class UserFaker(AggregateProvider[dict, User]):
    """Real UserFaker provider for sociable tests."""
    _id_attr = 'id'

    id: ValueProvider[int, UserId]
    status_id: ReferenceProvider
    name: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository,
            distributor: IM2ODistributor,
            status_provider: StatusFaker
    ):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor=True),
            input_generator=lambda session, pos=None: pos or 1,
            output_factory=UserId,
        )
        self.status_id = ReferenceProvider(
            distributor=distributor,
            aggregate_provider=status_provider,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(values=["Alice", "Bob", "Charlie"]),
            input_generator=lambda session, pos=None: "User %s" % (pos or 0),
        )
        super().__init__(
            repository=repository,
            output_factory=User,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': user.id.value if hasattr(user.id, 'value') else user.id,
            'status_id': user.status_id.value if hasattr(user.status_id, 'value') else user.status_id,
            'name': user.name,
        }


# =============================================================================
# Tests for ObjectPatternLookupSpecification - Basic
# =============================================================================

class ObjectPatternLookupSpecificationBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for ObjectPatternLookupSpecification."""

    async def test_is_satisfied_by_simple_pattern_without_provider(self):
        """Simple pattern matching without provider should work."""
        spec = ObjectPatternLookupSpecification(
            {'status': 'active'},
            lambda obj: obj
        )
        self.assertTrue(await spec.is_satisfied_by(None, {'status': 'active', 'name': 'test'}))
        self.assertFalse(await spec.is_satisfied_by(None, {'status': 'inactive', 'name': 'test'}))

    async def test_is_satisfied_by_nested_pattern_without_provider(self):
        """Nested pattern without provider should use simple subset check."""
        spec = ObjectPatternLookupSpecification(
            {'address': {'city': 'Moscow'}},
            lambda obj: obj
        )
        self.assertTrue(await spec.is_satisfied_by(None, {'address': {'city': 'Moscow', 'street': 'Main'}}))
        self.assertFalse(await spec.is_satisfied_by(None, {'address': {'city': 'London'}}))

    def test_hash_uses_object_pattern(self):
        """hash() should use _object_pattern."""
        spec = ObjectPatternLookupSpecification(
            {'status': 'active'},
            lambda obj: obj
        )

        from ascetic_ddd.seedwork.domain.utils.data import hashable
        expected_hash = hash(hashable({'status': 'active'}))
        self.assertEqual(hash(spec), expected_hash)

    def test_hash_equality(self):
        """Specifications with same _object_pattern should have equal hash."""
        spec1 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        self.assertEqual(hash(spec1), hash(spec2))

    def test_hash_inequality(self):
        """Specifications with different _object_pattern should have different hash."""
        spec1 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternLookupSpecification({'status': 'inactive'}, lambda obj: obj)
        self.assertNotEqual(hash(spec1), hash(spec2))

    def test_eq_uses_object_pattern(self):
        """__eq__ should compare _object_pattern."""
        spec1 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        self.assertEqual(spec1, spec2)

    def test_eq_different_patterns(self):
        """Specifications with different _object_pattern should not be equal."""
        spec1 = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternLookupSpecification({'status': 'inactive'}, lambda obj: obj)
        self.assertNotEqual(spec1, spec2)

    def test_str_uses_object_pattern(self):
        """__str__ should use _object_pattern."""
        spec = ObjectPatternLookupSpecification(
            {'status': 'active'},
            lambda obj: obj
        )

        from ascetic_ddd.seedwork.domain.utils.data import hashable
        expected_str = str(hashable({'status': 'active'}))
        self.assertEqual(str(spec), expected_str)

    def test_eq_with_non_specification(self):
        """__eq__ with non-ObjectPatternLookupSpecification should return False."""
        spec = ObjectPatternLookupSpecification({'status': 'active'}, lambda obj: obj)
        self.assertNotEqual(spec, {'status': 'active'})
        self.assertNotEqual(spec, "string")
        self.assertNotEqual(spec, 123)


# =============================================================================
# Tests for ObjectPatternLookupSpecification - Nested Lookup
# =============================================================================

class ObjectPatternLookupSpecificationNestedLookupTestCase(IsolatedAsyncioTestCase):
    """Tests for nested lookup in is_satisfied_by()."""

    def setUp(self):
        # Setup Status aggregate
        self.status_active = Status(StatusId("active"), "Active")
        self.status_inactive = Status(StatusId("inactive"), "Inactive")

        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)
        self.status_repo.add(self.status_inactive)

        self.status_provider = MockAggregateProvider(
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name},
            repository=self.status_repo
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
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            repository=self.user_repo
        )
        self.session = MockSession()

    async def test_nested_lookup_matches(self):
        """Nested lookup should match when foreign object satisfies pattern."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice has active status
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_alice))
        # Bob has inactive status
        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_bob))

    async def test_nested_lookup_returns_false_when_fk_is_none(self):
        """Nested lookup should return False when fk_id is None."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': None, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_alice))

    async def test_nested_lookup_returns_false_when_foreign_obj_not_found(self):
        """Nested lookup should return False when foreign object not found."""
        user_with_unknown_status = User(UserId(3), StatusId("unknown"), "Charlie")

        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.session, user_with_unknown_status))

    async def test_nested_lookup_with_non_reference_provider(self):
        """Non-reference provider should return True if fk_id is not None."""
        # name is not a ReferenceProvider, it's just a value
        user_provider = MockAggregateProvider(
            providers={'name': "not_a_reference_provider"},
            output_exporter=lambda u: {'id': u.id.value, 'name': u.name}
        )

        spec = ObjectPatternLookupSpecification(
            {'name': {'nested': 'value'}},  # name is not ReferenceProvider
            lambda u: {'id': u.id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: user_provider
        )

        # Should return True because fk_id ('Alice') is not None
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_alice))

    async def test_simple_value_comparison_with_provider(self):
        """Simple value comparison should work alongside nested lookup."""
        spec = ObjectPatternLookupSpecification(
            {'name': 'Alice', 'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice matches both name and status
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_alice))
        # Bob doesn't match name
        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_bob))

    async def test_simple_value_mismatch_with_provider(self):
        """Simple value mismatch should return False early."""
        spec = ObjectPatternLookupSpecification(
            {'name': 'NonExistent', 'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_alice))


# =============================================================================
# Tests for ObjectPatternLookupSpecification - Deep Nesting
# =============================================================================

class ObjectPatternLookupSpecificationDeepNestingTestCase(IsolatedAsyncioTestCase):
    """Tests for deeply nested lookup."""

    def setUp(self):
        # Setup Status aggregate
        self.status_active = Status(StatusId("active"), "Active")
        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)

        self.status_provider = MockAggregateProvider(
            providers={},
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name},
            repository=self.status_repo
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
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            repository=self.user_repo
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
            output_exporter=lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name},
            repository=self.company_repo
        )
        self.session = MockSession()

    async def test_two_level_nested_lookup(self):
        """Two-level nested lookup should work."""
        # Company -> User (owner) -> Status
        spec = ObjectPatternLookupSpecification(
            {'owner_id': {'status_id': {'name': 'Active'}}},
            lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name},
            aggregate_provider_accessor=lambda: self.company_provider
        )

        self.assertTrue(await spec.is_satisfied_by(self.session, self.company))

    async def test_two_level_nested_lookup_no_match(self):
        """Two-level nested lookup should return False when doesn't match."""
        spec = ObjectPatternLookupSpecification(
            {'owner_id': {'status_id': {'name': 'Inactive'}}},
            lambda c: {'id': c.id.value, 'owner_id': c.owner_id.value, 'name': c.name},
            aggregate_provider_accessor=lambda: self.company_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.session, self.company))


# =============================================================================
# Tests for ObjectPatternLookupSpecification - Cache
# =============================================================================

class ObjectPatternLookupSpecificationCacheTestCase(IsolatedAsyncioTestCase):
    """Tests for cache behavior."""

    def setUp(self):
        self.status_active = Status(StatusId("active"), "Active")
        self.status_repo = MockRepository()
        self.status_repo.add(self.status_active)

        self.status_provider = MockAggregateProvider(
            output_exporter=lambda s: {'id': s.id.value, 'name': s.name},
            repository=self.status_repo
        )

        self.user_alice = User(UserId(1), StatusId("active"), "Alice")

        self.status_ref_provider = MockReferenceProvider(
            self.status_repo, self.status_provider
        )

        self.user_provider = MockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name}
        )
        self.session = MockSession()

    async def test_cache_stores_result(self):
        """Cache should store lookup result."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertEqual(len(spec._nested_cache), 0)
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

    async def test_cache_key_includes_provider_type(self):
        """Cache key should include type(aggregate_provider)."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.is_satisfied_by(self.session, self.user_alice)

        cache_key = list(spec._nested_cache.keys())[0]
        self.assertEqual(cache_key[0], MockAggregateProvider)
        self.assertEqual(cache_key[1], 'status_id')
        self.assertEqual(cache_key[2], 'active')

    async def test_cache_hit_avoids_lookup(self):
        """Cache hit should avoid repository lookup."""
        call_count = [0]
        original_get = self.status_repo.get

        async def counting_get(session, id_):
            call_count[0] += 1
            return await original_get(session, id_)

        self.status_repo.get = counting_get

        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # First call - should hit repository
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(call_count[0], 1)

        # Second call - should use cache
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(call_count[0], 1)

    async def test_clear_cache(self):
        """clear_cache() should clear the cache."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

        spec.clear_cache()
        self.assertEqual(len(spec._nested_cache), 0)

    async def test_different_provider_types_have_different_cache_keys(self):
        """Different provider types should have different cache keys."""

        class AnotherMockAggregateProvider(MockAggregateProvider):
            pass

        another_provider = AnotherMockAggregateProvider(
            providers={'status_id': self.status_ref_provider},
            output_exporter=lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            repository=self.status_repo
        )

        provider_index = [0]
        providers = [self.user_provider, another_provider]

        def get_provider():
            return providers[provider_index[0]]

        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            lambda u: {'id': u.id.value, 'status_id': u.status_id.value, 'name': u.name},
            aggregate_provider_accessor=get_provider
        )

        # First provider
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

        # Second provider (different type)
        provider_index[0] = 1
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(len(spec._nested_cache), 2)


# =============================================================================
# Tests for ObjectPatternLookupSpecification - Accept
# =============================================================================

class ObjectPatternLookupSpecificationAcceptTestCase(IsolatedAsyncioTestCase):
    """Tests for accept() method."""

    def test_accept_passes_object_pattern(self):
        """accept() should pass _object_pattern to visitor."""
        received_pattern = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_pattern[0] = pattern

        original_pattern = {'status_id': {'name': 'Active'}}
        spec = ObjectPatternLookupSpecification(
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
        spec = ObjectPatternLookupSpecification(
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

        spec = ObjectPatternLookupSpecification(
            {'status': 'active'},
            lambda obj: obj,
            aggregate_provider_accessor=None
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        self.assertIsNone(received_accessor[0])


# =============================================================================
# Sociable Tests - Using Real Providers
# =============================================================================

class ObjectPatternLookupSpecificationSociableTestCase(IsolatedAsyncioTestCase):
    """
    Sociable tests using real AggregateProvider and ReferenceProvider.

    These tests verify that ObjectPatternLookupSpecification works correctly
    with the real provider infrastructure, not just mocks.
    """

    def setUp(self):
        # Create repositories using real InMemoryRepository
        self.status_repo = InMemoryRepository(
            agg_exporter=StatusFaker._export,
            id_attr='id'
        )
        self.user_repo = InMemoryRepository(
            agg_exporter=UserFaker._export,
            id_attr='id'
        )
        self.session = MockSession()

        # Create real StatusFaker provider
        self.status_distributor = StubDistributor(
            values=[
                Status(StatusId("active"), "Active"),
                Status(StatusId("inactive"), "Inactive"),
            ]
        )
        self.status_provider = StatusFaker(self.status_repo, self.status_distributor)
        self.status_provider.provider_name = "status"

        # Create real UserFaker provider with ReferenceProvider to Status
        self.user_distributor = StubDistributor()
        self.user_provider = UserFaker(
            self.user_repo,
            self.user_distributor,
            self.status_provider
        )
        self.user_provider.provider_name = "user"

        # Pre-populate repositories with test data
        self.status_active = Status(StatusId("active"), "Active")
        self.status_inactive = Status(StatusId("inactive"), "Inactive")

        self.user_alice = User(UserId(1), StatusId("active"), "Alice")
        self.user_bob = User(UserId(2), StatusId("inactive"), "Bob")

    async def asyncSetUp(self):
        await super().asyncSetUp()
        session = MockSession()

        # Insert statuses into real InMemoryRepository
        await self.status_repo.insert(session, self.status_active)
        await self.status_repo.insert(session, self.status_inactive)

        # Insert users
        await self.user_repo.insert(session, self.user_alice)
        await self.user_repo.insert(session, self.user_bob)

    async def test_nested_lookup_with_real_providers(self):
        """Nested lookup should work with real AggregateProvider and ReferenceProvider."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice has active status - should match
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_alice))

        # Bob has inactive status - should not match
        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_bob))

    async def test_nested_lookup_with_real_providers_inactive_status(self):
        """Nested lookup should correctly match inactive status."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Inactive'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice has active status - should not match
        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_alice))

        # Bob has inactive status - should match
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_bob))

    async def test_combined_pattern_with_real_providers(self):
        """Combined simple and nested pattern should work with real providers."""
        spec = ObjectPatternLookupSpecification(
            {'name': 'Alice', 'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # Alice matches both name and status
        self.assertTrue(await spec.is_satisfied_by(self.session, self.user_alice))

        # Bob doesn't match name
        self.assertFalse(await spec.is_satisfied_by(self.session, self.user_bob))

    async def test_cache_with_real_providers(self):
        """Cache should work correctly with real providers."""
        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        # First call populates cache
        await spec.is_satisfied_by(self.session, self.user_alice)
        self.assertEqual(len(spec._nested_cache), 1)

        # Verify cache key uses real provider type
        cache_key = list(spec._nested_cache.keys())[0]
        self.assertEqual(cache_key[0], UserFaker)

    async def test_foreign_object_not_found_with_real_providers(self):
        """Should return False when foreign object not in repository."""
        user_with_unknown_status = User(UserId(3), StatusId("unknown"), "Charlie")

        spec = ObjectPatternLookupSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.assertFalse(await spec.is_satisfied_by(self.session, user_with_unknown_status))
