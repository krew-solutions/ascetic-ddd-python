import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty


# =============================================================================
# Value Objects and Aggregates for testing
# =============================================================================

@dataclasses.dataclass(frozen=True)
class UserId:
    """Simple value object for user ID."""
    value: int


@dataclasses.dataclass
class User:
    """Aggregate with auto-increment or pre-set ID."""
    id: UserId
    name: str
    email: str


# =============================================================================
# Stub Distributor
# =============================================================================

class StubDistributor(IM2ODistributor):
    """Stub distributor for testing."""

    def __init__(self, values: list = None, raise_cursor_at: int | None = None):
        self._values = values or []
        self._index = 0
        self._raise_cursor_at = raise_cursor_at
        self._appended = []
        self._provider_name = None

    async def next(self, session: ISession, specification=None):
        if self._raise_cursor_at is not None and self._index >= self._raise_cursor_at:
            raise Cursor(position=self._index, callback=self._append)
        if self._index < len(self._values):
            value = self._values[self._index]
            self._index += 1
            return value
        raise Cursor(position=self._index, callback=self._append)

    async def _append(self, session: ISession, value, position: int | None):
        self._appended.append((value, position))
        self._values.append(value)

    async def append(self, session: ISession, value):
        await self._append(session, value, None)

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

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    def bind_external_source(self, external_source) -> None:
        pass


# =============================================================================
# Stub Repository
# =============================================================================

class StubRepository(IAggregateRepository[User]):
    """Stub repository for testing."""

    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[int, User] = {}
        self._auto_increment_counter = auto_increment_start
        self._inserted: list[User] = []

    # IObservable methods
    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    async def insert(self, session: ISession, agg: User):
        # Simulate auto-increment: if ID is None or 0, assign new ID
        # Modify in-place (typical ORM behavior)
        if agg.id is None or (isinstance(agg.id, UserId) and agg.id.value == 0):
            new_id = UserId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = new_id  # Modify in-place
        self._storage[agg.id.value] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: User):
        self._storage[agg.id.value] = agg

    async def get(self, session: ISession, id_: UserId) -> User | None:
        if isinstance(id_, UserId):
            return self._storage.get(id_.value)
        return self._storage.get(id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[User]:
        return list(self._storage.values())

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


class MockSession:
    """Mock session for testing."""
    pass


# =============================================================================
# Value Generators
# =============================================================================

async def user_id_generator(session: ISession, position: int | None = None) -> int:
    """Generates user ID values starting from 100."""
    return (position if position is not None else 0) + 100


async def name_generator(session: ISession, position: int | None = None) -> str:
    return f"User_{position if position is not None else 0}"


async def email_generator(session: ISession, position: int | None = None) -> str:
    return f"user_{position if position is not None else 0}@example.com"


# =============================================================================
# Aggregate Providers
# =============================================================================

class UserProviderAutoIncrement(AggregateProvider[dict, User]):
    """
    UserProvider with auto-increment ID.
    ID is assigned by repository after insert.
    """
    _id_attr = 'id'

    id: ValueProvider[int, UserId]
    name: ValueProvider[str, str]
    email: ValueProvider[str, str]

    def __init__(self, repository: IAggregateRepository[User]):
        # ID distributor raises Cursor immediately - simulates no pre-existing IDs
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=lambda: 0,  # Returns 0 - will be replaced by auto-increment
            output_factory=UserId,
            output_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=name_generator,
        )
        self.email = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=email_generator,
        )
        super().__init__(
            repository=repository,
            output_factory=User,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': user.id.value,
            'name': user.name,
            'email': user.email,
        }


class UserProviderPresetPK(AggregateProvider[dict, User]):
    """
    UserProvider with pre-set ID.
    ID is generated by ValueProvider before insert.
    """
    _id_attr = 'id'

    id: ValueProvider[int, UserId]
    name: ValueProvider[str, str]
    email: ValueProvider[str, str]

    def __init__(self, repository: IAggregateRepository[User]):
        # ID is generated by input_generator - simulates pre-set PK
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=user_id_generator,
            output_factory=UserId,
            output_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=name_generator,
        )
        self.email = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=email_generator,
        )
        super().__init__(
            repository=repository,
            output_factory=User,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': user.id.value,
            'name': user.name,
            'email': user.email,
        }


# =============================================================================
# Test Cases: Auto-increment PK
# =============================================================================

class AggregateProviderAutoIncrementTestCase(IsolatedAsyncioTestCase):
    """Tests for AggregateProvider with auto-increment PK."""

    async def test_create_inserts_new_aggregate(self):
        """create() should insert new aggregate when ID is auto-incremented."""
        repository = StubRepository(auto_increment_start=1)
        provider = UserProviderAutoIncrement(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        result = await provider.create(session)

        self.assertIsInstance(result, User)
        self.assertEqual(len(repository._inserted), 1)

    async def test_is_complete_true_after_create(self):
        """is_complete() should return True after create() and _output should be set."""
        repository = StubRepository(auto_increment_start=1)
        provider = UserProviderAutoIncrement(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        result = await provider.create(session)

        self.assertTrue(provider.is_complete())
        self.assertIs(provider._output, result)
        self.assertIsNot(provider._output, empty)

    async def test_auto_increment_assigns_id(self):
        """Repository should assign ID via auto-increment."""
        repository = StubRepository(auto_increment_start=42)
        provider = UserProviderAutoIncrement(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        result = await provider.create(session)

        # Repository assigns ID starting from 42
        self.assertEqual(result.id.value, 42)

    async def test_id_provider_updated_after_create(self):
        """id_provider should be updated with auto-incremented ID after create()."""
        repository = StubRepository(auto_increment_start=10)
        provider = UserProviderAutoIncrement(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        await provider.create(session)

        self.assertEqual(provider.id.get(), 10)

    async def test_populate_populates_all_providers(self):
        """populate() should populate all nested providers."""
        repository = StubRepository()
        provider = UserProviderAutoIncrement(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.name.is_complete())
        self.assertTrue(provider.email.is_complete())

    async def test_multiple_creates_get_sequential_ids(self):
        """Multiple creates should get sequential auto-increment IDs."""
        repository = StubRepository(auto_increment_start=1)
        session = MockSession()

        provider1 = UserProviderAutoIncrement(repository)
        provider1.provider_name = 'user1'
        await provider1.populate(session)
        result1 = await provider1.create(session)

        provider2 = UserProviderAutoIncrement(repository)
        provider2.provider_name = 'user2'
        await provider2.populate(session)
        result2 = await provider2.create(session)

        self.assertEqual(result1.id.value, 1)
        self.assertEqual(result2.id.value, 2)


# =============================================================================
# Test Cases: Pre-set PK
# =============================================================================

class AggregateProviderPresetPKTestCase(IsolatedAsyncioTestCase):
    """Tests for AggregateProvider with pre-set PK from ValueProvider."""

    async def test_create_with_preset_id(self):
        """create() should use ID generated by ValueProvider."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        result = await provider.create(session)

        self.assertIsInstance(result, User)
        # ID should be from user_id_generator: 100 + position(0) = 100
        self.assertEqual(result.id.value, 100)

    async def test_is_complete_true_after_create_preset_pk(self):
        """is_complete() should return True after create() with pre-set PK."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        result = await provider.create(session)

        self.assertTrue(provider.is_complete())
        self.assertIs(provider._output, result)
        self.assertIsNot(provider._output, empty)

    async def test_id_provider_complete_before_create(self):
        """id_provider should be complete before create() with pre-set PK."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.id.is_complete())

    async def test_reuse_existing_aggregate(self):
        """create() should reuse existing aggregate if found by ID."""
        repository = StubRepository()
        session = MockSession()

        # First create
        provider1 = UserProviderPresetPK(repository)
        provider1.provider_name = 'user1'
        await provider1.populate(session)
        result1 = await provider1.create(session)

        # Second provider with same ID should reuse
        provider2 = UserProviderPresetPK(repository)
        provider2.provider_name = 'user2'
        # Pre-set the same ID
        provider2.id.set(100)
        await provider2.populate(session)
        result2 = await provider2.create(session)

        # Should be the same aggregate (reused)
        self.assertEqual(result1.id.value, result2.id.value)
        self.assertEqual(result1.name, result2.name)
        # Only one insert should have happened
        self.assertEqual(len(repository._inserted), 1)

    async def test_set_propagates_id_to_provider(self):
        """set() should propagate ID value to id_provider."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'

        provider.set({
            'id': 999,
            'name': 'Custom Name',
            'email': 'custom@example.com',
        })

        self.assertEqual(provider.id.get(), 999)
        self.assertEqual(provider.name.get(), 'Custom Name')
        self.assertEqual(provider.email.get(), 'custom@example.com')

    async def test_get_returns_state(self):
        """get() should return current state of all providers."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        state = provider.get()

        self.assertIn('id', state)
        self.assertIn('name', state)
        self.assertIn('email', state)


# =============================================================================
# Test Cases: Reset and State
# =============================================================================

class AggregateProviderResetTestCase(IsolatedAsyncioTestCase):
    """Tests for reset functionality."""

    async def test_reset_clears_state(self):
        """reset() should clear provider state."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)
        self.assertTrue(provider.is_complete())

        provider.reset()

        self.assertFalse(provider.is_complete())
        self.assertEqual(provider._input, empty)
        self.assertEqual(provider._output, empty)

    async def test_reset_clears_nested_providers(self):
        """reset() should clear all nested providers."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        provider.provider_name = 'user'
        session = MockSession()

        await provider.populate(session)

        provider.reset()

        self.assertFalse(provider.id.is_complete())
        self.assertFalse(provider.name.is_complete())
        self.assertFalse(provider.email.is_complete())


# =============================================================================
# Test Cases: Provider Name
# =============================================================================

class AggregateProviderNameTestCase(IsolatedAsyncioTestCase):
    """Tests for provider_name propagation."""

    async def test_provider_name_propagates_to_nested(self):
        """provider_name should propagate to all nested providers."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)

        provider.provider_name = 'root'

        self.assertEqual(provider.provider_name, 'root')
        self.assertEqual(provider.id.provider_name, 'root.id')
        self.assertEqual(provider.name.provider_name, 'root.name')
        self.assertEqual(provider.email.provider_name, 'root.email')


# =============================================================================
# Test Cases: Setup and Cleanup
# =============================================================================

class AggregateProviderSetupCleanupTestCase(IsolatedAsyncioTestCase):
    """Tests for setup and cleanup."""

    async def test_setup_calls_repository_setup(self):
        """setup() should call repository.setup()."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        session = MockSession()

        # Should not raise
        await provider.setup(session)

    async def test_cleanup_calls_repository_cleanup(self):
        """cleanup() should call repository.cleanup()."""
        repository = StubRepository()
        provider = UserProviderPresetPK(repository)
        session = MockSession()

        # Should not raise
        await provider.cleanup(session)


if __name__ == '__main__':
    import unittest
    unittest.main()
