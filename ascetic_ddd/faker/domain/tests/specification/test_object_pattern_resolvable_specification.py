import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.object_pattern_resolvable_specification import ObjectPatternResolvableSpecification
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


class StubRepository(IAggregateRepository):
    """Stub repository for testing."""

    def __init__(self):
        self._storage = {}
        self._observers = []

    async def insert(self, session: ISession, agg):
        key = self._get_key(agg)
        self._storage[key] = agg

    async def get(self, session: ISession, id_) -> typing.Any:
        key = self._extract_key(id_)
        return self._storage.get(key)

    async def update(self, session: ISession, agg):
        key = self._get_key(agg)
        self._storage[key] = agg

    async def find(self, session: ISession, specification: ISpecification):
        return list(self._storage.values())

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        self._storage.clear()

    def _get_key(self, agg):
        if hasattr(agg, 'id'):
            return self._extract_key(agg.id)
        return id(agg)

    def _extract_key(self, id_):
        """Extract hashable key from id."""
        if hasattr(id_, 'value'):
            return id_.value
        if isinstance(id_, dict):
            return tuple(sorted(id_.items()))
        return id_

    def attach(self, aspect, observer, id_=None):
        self._observers.append((aspect, observer))
        return lambda: self._observers.remove((aspect, observer))

    def detach(self, aspect, observer, id_=None):
        self._observers = [(a, o) for a, o in self._observers if o != observer]

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass


# =============================================================================
# Providers
# =============================================================================

class StatusFaker(AggregateProvider[dict, Status]):
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
# Tests for ObjectPatternResolvableSpecification
# =============================================================================

class ObjectPatternResolvableSpecificationBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for ObjectPatternResolvableSpecification."""

    async def test_is_satisfied_by_simple_pattern(self):
        """Simple pattern matching should work."""
        spec = ObjectPatternResolvableSpecification(
            {'status': 'active'},
            lambda obj: obj
        )
        session = MockSession()
        await spec.resolve_nested(session)
        self.assertTrue(await spec.is_satisfied_by(session, {'status': 'active', 'name': 'test'}))
        self.assertFalse(await spec.is_satisfied_by(session, {'status': 'inactive', 'name': 'test'}))

    async def test_is_satisfied_by_nested_pattern(self):
        """Nested pattern matching should work."""
        spec = ObjectPatternResolvableSpecification(
            {'address': {'city': 'Moscow'}},
            lambda obj: obj
        )
        session = MockSession()
        await spec.resolve_nested(session)
        self.assertTrue(await spec.is_satisfied_by(session, {'address': {'city': 'Moscow', 'street': 'Main'}}))
        self.assertFalse(await spec.is_satisfied_by(session, {'address': {'city': 'London'}}))

    async def test_is_satisfied_by_unresolved_raises_exception(self):
        """is_satisfied_by() on unresolved specification should raise TypeError."""
        spec = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        session = MockSession()
        with self.assertRaises(TypeError) as ctx:
            await spec.is_satisfied_by(session, {'status': 'active'})
        self.assertIn("unresolved", str(ctx.exception))

    async def test_hash_equality(self):
        """Specifications with same resolved pattern should be equal."""
        spec1 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        session = MockSession()
        await spec1.resolve_nested(session)
        await spec2.resolve_nested(session)
        self.assertEqual(hash(spec1), hash(spec2))
        self.assertEqual(spec1, spec2)

    async def test_hash_inequality(self):
        """Specifications with different resolved patterns should not be equal."""
        spec1 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternResolvableSpecification({'status': 'inactive'}, lambda obj: obj)
        session = MockSession()
        await spec1.resolve_nested(session)
        await spec2.resolve_nested(session)
        self.assertNotEqual(hash(spec1), hash(spec2))
        self.assertNotEqual(spec1, spec2)

    def test_hash_unresolved_raises_exception(self):
        """Hash of unresolved specification should raise TypeError."""
        spec = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        with self.assertRaises(TypeError) as ctx:
            hash(spec)
        self.assertIn("unresolved", str(ctx.exception))

    def test_eq_unresolved_raises_exception(self):
        """Comparing unresolved specifications should raise TypeError."""
        spec1 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        with self.assertRaises(TypeError) as ctx:
            spec1 == spec2
        self.assertIn("unresolved", str(ctx.exception))

    async def test_hash_uses_resolved_pattern_not_object_pattern(self):
        """hash() should use _resolved_pattern, not _object_pattern."""
        spec = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        session = MockSession()
        await spec.resolve_nested(session)

        # Manually change _resolved_pattern to verify hash uses it
        spec._resolved_pattern = {'status': 'modified'}
        spec._hash = None  # Reset cached hash

        from ascetic_ddd.seedwork.domain.utils.data import hashable
        expected_hash = hash(hashable({'status': 'modified'}))
        self.assertEqual(hash(spec), expected_hash)

    async def test_eq_uses_resolved_pattern_not_object_pattern(self):
        """__eq__() should compare _resolved_pattern, not _object_pattern."""
        # Same _object_pattern but different _resolved_pattern
        spec1 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        spec2 = ObjectPatternResolvableSpecification({'status': 'active'}, lambda obj: obj)
        session = MockSession()
        await spec1.resolve_nested(session)
        await spec2.resolve_nested(session)

        # Manually set different _resolved_pattern
        spec1._resolved_pattern = {'status': 'value1'}
        spec2._resolved_pattern = {'status': 'value2'}

        self.assertNotEqual(spec1, spec2)

        # Same _resolved_pattern
        spec2._resolved_pattern = {'status': 'value1'}
        self.assertEqual(spec1, spec2)

    async def test_is_satisfied_by_uses_resolved_pattern_not_object_pattern(self):
        """is_satisfied_by() should use _resolved_pattern, not _object_pattern."""
        spec = ObjectPatternResolvableSpecification(
            {'status': 'original'},
            lambda obj: obj
        )
        session = MockSession()
        await spec.resolve_nested(session)

        # Manually set different _resolved_pattern
        spec._resolved_pattern = {'status': 'resolved'}

        # Should match against _resolved_pattern, not _object_pattern
        self.assertTrue(await spec.is_satisfied_by(session, {'status': 'resolved', 'extra': 'field'}))
        self.assertFalse(await spec.is_satisfied_by(session, {'status': 'original'}))


class ObjectPatternResolvableSpecificationResolveNestedTestCase(IsolatedAsyncioTestCase):
    """Tests for ObjectPatternResolvableSpecification.resolve_nested()."""

    async def test_resolve_nested_without_accessor(self):
        """Without aggregate_provider_accessor, pattern stays unchanged."""
        spec = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            lambda obj: obj,
            aggregate_provider_accessor=None
        )

        session = MockSession()
        await spec.resolve_nested(session)

        # Pattern should be unchanged (copied to _resolved_pattern)
        self.assertEqual(spec._resolved_pattern, {'status_id': {'name': 'Active'}})

    async def test_resolve_nested_simple_values_unchanged(self):
        """Simple (non-dict) values should stay unchanged."""
        status_repo = StubRepository()
        status_dist = StubDistributor(values=[Status(StatusId("active"), "Active")])
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        user_repo = StubRepository()
        user_dist = StubDistributor()
        user_provider = UserFaker(user_repo, user_dist, status_provider)
        user_provider.provider_name = "user"

        spec = ObjectPatternResolvableSpecification(
            {'name': 'Alice', 'id': 123},
            lambda obj: obj,
            aggregate_provider_accessor=lambda: user_provider
        )

        session = MockSession()
        await spec.resolve_nested(session)

        # Simple values should be unchanged
        self.assertEqual(spec._resolved_pattern['name'], 'Alice')
        self.assertEqual(spec._resolved_pattern['id'], 123)

    async def test_resolve_nested_with_reference_provider(self):
        """Nested dict for IReferenceProvider should be resolved to ID."""
        from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider

        # Create a mock reference provider that returns a known ID
        class MockReferenceProvider(IReferenceProvider):
            def __init__(self):
                self._input = empty
                self._output = empty
                self._provider_name = None

            async def populate(self, session):
                # Simulate populating with resolved ID
                self._output = StatusId("resolved_active")

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
                return MockReferenceProvider()

            def do_empty(self, clone, shunt):
                pass

            @property
            def provider_name(self):
                return self._provider_name

            @provider_name.setter
            def provider_name(self, value):
                self._provider_name = value

            @property
            def aggregate_provider(self):
                return None

            @aggregate_provider.setter
            def aggregate_provider(self, value):
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

        # Create a mock aggregate provider with the reference provider
        class MockAggregateProvider:
            def __init__(self):
                self.status_id = MockReferenceProvider()

            @property
            def providers(self):
                return {'status_id': self.status_id}

        mock_agg_provider = MockAggregateProvider()

        # Create specification with nested constraint
        spec = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            lambda obj: obj,
            aggregate_provider_accessor=lambda: mock_agg_provider
        )

        session = MockSession()
        await spec.resolve_nested(session)

        # Nested dict should be resolved to concrete ID
        self.assertIn('status_id', spec._resolved_pattern)
        resolved_status_id = spec._resolved_pattern['status_id']
        self.assertEqual(resolved_status_id, StatusId("resolved_active"))

    async def test_resolve_nested_idempotent(self):
        """Calling resolve_nested() multiple times should be idempotent."""
        spec = ObjectPatternResolvableSpecification(
            {'status': 'active'},
            lambda obj: obj,
            aggregate_provider_accessor=None
        )

        session = MockSession()
        await spec.resolve_nested(session)
        first_resolved = spec._resolved_pattern

        await spec.resolve_nested(session)
        second_resolved = spec._resolved_pattern

        # Should be the same object (not re-resolved)
        self.assertIs(first_resolved, second_resolved)

    async def test_resolve_nested_non_reference_provider_unchanged(self):
        """Nested dict for non-IReferenceProvider should stay as dict."""
        status_repo = StubRepository()
        status_dist = StubDistributor(values=[Status(StatusId("active"), "Active")])
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        user_repo = StubRepository()
        user_dist = StubDistributor()
        user_provider = UserFaker(user_repo, user_dist, status_provider)
        user_provider.provider_name = "user"

        # 'name' is a ValueProvider, not ReferenceProvider
        # So nested dict should stay unchanged
        spec = ObjectPatternResolvableSpecification(
            {'name': {'nested': 'value'}},  # name is ValueProvider
            user_provider._output_exporter,
            aggregate_provider_accessor=lambda: user_provider
        )

        session = MockSession()
        await spec.resolve_nested(session)

        # Should stay as dict (ValueProvider doesn't resolve nested)
        self.assertEqual(spec._resolved_pattern['name'], {'nested': 'value'})


class ObjectPatternResolvableSpecificationAcceptTestCase(IsolatedAsyncioTestCase):
    """Tests for ObjectPatternResolvableSpecification.accept()."""

    def test_accept_passes_aggregate_provider_accessor(self):
        """accept() should pass aggregate_provider_accessor to visitor."""
        received_accessor = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_accessor[0] = accessor

        accessor = lambda: "test_provider"
        spec = ObjectPatternResolvableSpecification(
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

        spec = ObjectPatternResolvableSpecification(
            {'status': 'active'},
            lambda obj: obj,
            aggregate_provider_accessor=None
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        self.assertIsNone(received_accessor[0])

    def test_accept_passes_object_pattern_when_unresolved(self):
        """accept() should pass _object_pattern when specification is not resolved."""
        received_pattern = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_pattern[0] = pattern

        original_pattern = {'status_id': {'name': 'Active'}}
        spec = ObjectPatternResolvableSpecification(
            original_pattern,
            lambda obj: obj
        )

        visitor = MockVisitor()
        spec.accept(visitor)

        # Should pass original pattern (with nested dict)
        self.assertEqual(received_pattern[0], original_pattern)
        self.assertIs(received_pattern[0], original_pattern)

    async def test_accept_passes_resolved_pattern_when_resolved(self):
        """accept() should pass _resolved_pattern when specification is resolved."""
        received_pattern = [None]

        class MockVisitor:
            def visit_object_pattern_specification(self, pattern, accessor=None):
                received_pattern[0] = pattern

        original_pattern = {'status': 'active'}
        spec = ObjectPatternResolvableSpecification(
            original_pattern,
            lambda obj: obj
        )

        session = MockSession()
        await spec.resolve_nested(session)

        visitor = MockVisitor()
        spec.accept(visitor)

        # Should pass resolved pattern
        self.assertEqual(received_pattern[0], spec._resolved_pattern)
        self.assertIs(received_pattern[0], spec._resolved_pattern)


# =============================================================================
# Sociable Tests — тесты с реальными коллабораторами
# =============================================================================

class ObjectPatternResolvableSpecificationSociableTestCase(IsolatedAsyncioTestCase):
    """Sociable tests with real collaborators (InMemoryRepository, real providers)."""

    async def asyncSetUp(self):
        """Set up real repositories and providers."""
        self.session = MockSession()

        # Real repositories
        self.status_repo = InMemoryRepository(
            agg_exporter=StatusFaker._export,
            id_attr='id',
        )
        self.user_repo = InMemoryRepository(
            agg_exporter=UserFaker._export,
            id_attr='id',
        )

        await self.status_repo.setup(self.session)
        await self.user_repo.setup(self.session)

        # Real providers with StubDistributor (external dependency)
        self.status_dist = StubDistributor(raise_cursor=True)
        self.status_provider = StatusFaker(self.status_repo, self.status_dist)
        self.status_provider.provider_name = "status"

        self.user_dist = StubDistributor(raise_cursor=True)
        self.user_provider = UserFaker(self.user_repo, self.user_dist, self.status_provider)
        self.user_provider.provider_name = "user"

    async def asyncTearDown(self):
        """Cleanup repositories."""
        await self.status_repo.cleanup(self.session)
        await self.user_repo.cleanup(self.session)

    async def test_resolve_nested_returns_exported_dict(self):
        """resolve_nested() with real ReferenceProvider returns exported dict, not ID.

        This documents the actual behavior: ReferenceProvider.get() returns
        the full exported dict, not just the ID value.
        """
        # Pre-populate user_dist (used by UserFaker.status_id ReferenceProvider)
        # with Status aggregate
        active_status = Status(StatusId("active"), "Active")
        await self.status_repo.insert(self.session, active_status)
        # user_dist is used by ReferenceProvider, returns Status objects
        self.user_dist._values = [active_status]
        self.user_dist._raise_cursor = False
        self.user_dist._index = 0

        spec = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.resolve_nested(self.session)

        # ReferenceProvider.get() returns exported dict, not StatusId
        resolved_status = spec._resolved_pattern['status_id']
        self.assertIsInstance(resolved_status, dict)
        self.assertEqual(resolved_status['id'], 'active')
        self.assertEqual(resolved_status['name'], 'Active')

    async def test_resolve_nested_creates_new_aggregate_on_cursor(self):
        """When distributor raises Cursor, a new aggregate is created."""
        # Distributor with raise_cursor=True - will create new Status
        self.status_dist._raise_cursor = True

        spec = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.resolve_nested(self.session)

        # New Status was created with generated ID
        resolved_status = spec._resolved_pattern['status_id']
        self.assertIsInstance(resolved_status, dict)
        self.assertEqual(resolved_status['name'], 'Active')
        # ID is auto-generated (status_0, status_1, etc.)
        self.assertTrue(resolved_status['id'].startswith('status_'))

    async def test_simple_pattern_with_real_providers(self):
        """Simple patterns work with real providers."""
        user = User(UserId(1), StatusId("active"), "Alice")
        await self.user_repo.insert(self.session, user)

        spec = ObjectPatternResolvableSpecification(
            {'name': 'Alice'},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.resolve_nested(self.session)

        self.assertTrue(await spec.is_satisfied_by(self.session, user))

        user2 = User(UserId(2), StatusId("active"), "Bob")
        self.assertFalse(await spec.is_satisfied_by(self.session, user2))

    async def test_hash_equality_with_real_providers(self):
        """Hash and equality work with real providers after resolve_nested()."""
        # Pre-populate user_dist (used by ReferenceProvider)
        active_status = Status(StatusId("active"), "Active")
        await self.status_repo.insert(self.session, active_status)
        self.user_dist._values = [active_status, active_status]  # для обоих spec
        self.user_dist._raise_cursor = False

        spec1 = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )
        spec2 = ObjectPatternResolvableSpecification(
            {'status_id': {'name': 'Active'}},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        self.user_dist._index = 0
        await spec1.resolve_nested(self.session)

        # Reset provider state for second spec
        self.user_provider.status_id.reset()
        self.user_dist._index = 0
        await spec2.resolve_nested(self.session)

        # Same resolved pattern → equal hash
        self.assertEqual(spec1._resolved_pattern, spec2._resolved_pattern)
        self.assertEqual(hash(spec1), hash(spec2))
        self.assertEqual(spec1, spec2)

    async def test_repository_find_with_specification(self):
        """InMemoryRepository.find() works with specification."""
        user1 = User(UserId(1), StatusId("active"), "Alice")
        user2 = User(UserId(2), StatusId("inactive"), "Bob")
        user3 = User(UserId(3), StatusId("active"), "Alice")
        await self.user_repo.insert(self.session, user1)
        await self.user_repo.insert(self.session, user2)
        await self.user_repo.insert(self.session, user3)

        spec = ObjectPatternResolvableSpecification(
            {'name': 'Alice'},
            UserFaker._export,
            aggregate_provider_accessor=lambda: self.user_provider
        )

        await spec.resolve_nested(self.session)

        found = [u async for u in self.user_repo.find(self.session, spec)]

        self.assertEqual(len(found), 2)
        names = [u.name for u in found]
        self.assertIn('Alice', names)
        self.assertNotIn('Bob', names)
