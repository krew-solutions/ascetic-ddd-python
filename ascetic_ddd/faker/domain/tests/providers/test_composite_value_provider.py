import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.composite_value_provider import CompositeValueProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty


# =============================================================================
# Value Objects for testing multi-level composition
# =============================================================================

@dataclasses.dataclass(frozen=True)
class TenantId:
    """Level 1: Simple value object wrapping int."""
    value: int


@dataclasses.dataclass(frozen=True)
class InternalUserId:
    """Level 1: Simple value object wrapping int."""
    value: int


@dataclasses.dataclass(frozen=True)
class InternalResumeId:
    """Level 1: Simple value object wrapping int."""
    value: int


@dataclasses.dataclass(frozen=True)
class UserId:
    """Level 2: Composite value object containing TenantId and InternalUserId."""
    tenant_id: TenantId
    internal_user_id: InternalUserId


@dataclasses.dataclass(frozen=True)
class ResumeId:
    """Level 3: Composite value object containing UserId and InternalResumeId."""
    user_id: UserId
    internal_resume_id: InternalResumeId


# =============================================================================
# Mock Distributor (Stub)
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

    def bind_external_source(self, external_source: typing.Any) -> None:
        pass


class MockSession:
    """Mock session for testing."""
    pass


# =============================================================================
# Value Generators
# =============================================================================

async def tenant_id_generator(session: ISession, position: int | None = None) -> int:
    return position if position is not None else 1


async def internal_user_id_generator(session: ISession, position: int | None = None) -> int:
    return (position if position is not None else 1) + 100


async def internal_resume_id_generator(session: ISession, position: int | None = None) -> int:
    return (position if position is not None else 1) + 1000


# =============================================================================
# Composite Providers (Level 2+)
# =============================================================================

class UserIdProvider(CompositeValueProvider[dict, UserId]):
    """Provider for UserId - contains TenantId and InternalUserId."""
    tenant_id: ValueProvider[int, TenantId]
    internal_user_id: ValueProvider[int, InternalUserId]

    def __init__(self, distributor: IM2ODistributor):
        self.tenant_id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=tenant_id_generator,
            output_factory=TenantId,
            output_exporter=lambda x: x.value,
        )
        self.internal_user_id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=internal_user_id_generator,
            output_factory=InternalUserId,
            output_exporter=lambda x: x.value,
        )
        super().__init__(
            distributor=distributor,
            output_factory=UserId,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(user_id: UserId) -> dict:
        return {
            'tenant_id': user_id.tenant_id.value,
            'internal_user_id': user_id.internal_user_id.value,
        }


class ResumeIdProvider(CompositeValueProvider[dict, ResumeId]):
    """Provider for ResumeId - contains UserId and InternalResumeId."""
    user_id: UserIdProvider
    internal_resume_id: ValueProvider[int, InternalResumeId]

    def __init__(self, distributor: IM2ODistributor):
        self.user_id = UserIdProvider(StubDistributor(raise_cursor_at=0))
        self.internal_resume_id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            input_generator=internal_resume_id_generator,
            output_factory=InternalResumeId,
            output_exporter=lambda x: x.value,
        )
        super().__init__(
            distributor=distributor,
            output_factory=ResumeId,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(resume_id: ResumeId) -> dict:
        return {
            'user_id': UserIdProvider._export(resume_id.user_id),
            'internal_resume_id': resume_id.internal_resume_id.value,
        }


# =============================================================================
# Test Cases
# =============================================================================

class CompositeValueProviderLevel2TestCase(IsolatedAsyncioTestCase):
    """Tests for Level 2: UserId = TenantId + InternalUserId."""

    async def test_populate_creates_user_id(self):
        """UserIdProvider should create UserId with nested TenantId and InternalUserId."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        result = await provider.create(session)
        self.assertIsInstance(result, UserId)
        self.assertIsInstance(result.tenant_id, TenantId)
        self.assertIsInstance(result.internal_user_id, InternalUserId)

    async def test_output_set_after_populate_with_cursor(self):
        """_output should be set after populate() when ICursor is raised."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertIsNot(provider._output, empty)
        self.assertIsInstance(provider._output, UserId)

    async def test_nested_providers_are_populated(self):
        """Nested ValueProviders should be populated during parent populate()."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.tenant_id.is_complete())
        self.assertTrue(provider.internal_user_id.is_complete())

    async def test_set_propagates_to_nested_providers(self):
        """set() should propagate values to nested ValueProviders."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'

        provider.set({
            'tenant_id': 99,
            'internal_user_id': 199,
        })

        self.assertEqual(provider.tenant_id.get(), 99)
        self.assertEqual(provider.internal_user_id.get(), 199)

    async def test_reuse_existing_user_id(self):
        """UserIdProvider should reuse existing UserId from distributor."""
        existing = UserId(
            tenant_id=TenantId(value=42),
            internal_user_id=InternalUserId(value=142),
        )
        distributor = StubDistributor(values=[existing])
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'
        session = MockSession()

        await provider.populate(session)

        result = await provider.create(session)
        self.assertEqual(result, existing)


class CompositeValueProviderLevel3TestCase(IsolatedAsyncioTestCase):
    """Tests for Level 3: ResumeId = UserId + InternalResumeId."""

    async def test_populate_creates_resume_id(self):
        """ResumeIdProvider should create ResumeId with deep nested structure."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = ResumeIdProvider(distributor)
        provider.provider_name = 'resume_id'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        result = await provider.create(session)
        self.assertIsInstance(result, ResumeId)
        self.assertIsInstance(result.user_id, UserId)
        self.assertIsInstance(result.user_id.tenant_id, TenantId)
        self.assertIsInstance(result.user_id.internal_user_id, InternalUserId)
        self.assertIsInstance(result.internal_resume_id, InternalResumeId)

    async def test_all_nested_providers_are_populated(self):
        """All levels of nested providers should be populated."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = ResumeIdProvider(distributor)
        provider.provider_name = 'resume_id'
        session = MockSession()

        await provider.populate(session)

        # Level 2 nested ValueProviders
        self.assertTrue(provider.user_id.tenant_id.is_complete())
        self.assertTrue(provider.user_id.internal_user_id.is_complete())
        self.assertTrue(provider.internal_resume_id.is_complete())
        # Level 2 CompositeProvider
        self.assertTrue(provider.user_id.is_complete())
        # Level 3
        self.assertTrue(provider.is_complete())

    async def test_deep_set_propagates_to_all_levels(self):
        """set() should propagate values through all nesting levels."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = ResumeIdProvider(distributor)
        provider.provider_name = 'resume_id'

        provider.set({
            'user_id': {
                'tenant_id': 1,
                'internal_user_id': 101,
            },
            'internal_resume_id': 1001,
        })

        self.assertEqual(provider.user_id.tenant_id.get(), 1)
        self.assertEqual(provider.user_id.internal_user_id.get(), 101)
        self.assertEqual(provider.internal_resume_id.get(), 1001)

    async def test_get_returns_nested_structure(self):
        """get() should return the full nested structure."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = ResumeIdProvider(distributor)
        provider.provider_name = 'resume_id'
        session = MockSession()

        await provider.populate(session)

        result = provider.get()
        self.assertIn('user_id', result)
        self.assertIn('internal_resume_id', result)
        self.assertIn('tenant_id', result['user_id'])
        self.assertIn('internal_user_id', result['user_id'])

    async def test_reuse_existing_resume_id(self):
        """ResumeIdProvider should reuse existing ResumeId from distributor."""
        existing = ResumeId(
            user_id=UserId(
                tenant_id=TenantId(value=5),
                internal_user_id=InternalUserId(value=105),
            ),
            internal_resume_id=InternalResumeId(value=1005),
        )
        distributor = StubDistributor(values=[existing])
        provider = ResumeIdProvider(distributor)
        provider.provider_name = 'resume_id'
        session = MockSession()

        await provider.populate(session)

        result = await provider.create(session)
        self.assertEqual(result, existing)


class CompositeValueProviderResetTestCase(IsolatedAsyncioTestCase):
    """Tests for reset functionality."""

    async def test_reset_clears_state(self):
        """reset() should clear provider state."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = UserIdProvider(distributor)
        provider.provider_name = 'user_id'
        session = MockSession()

        await provider.populate(session)
        self.assertTrue(provider.is_complete())

        provider.reset()

        self.assertFalse(provider.is_complete())
        self.assertEqual(provider._input, empty)
        self.assertEqual(provider._output, empty)


class CompositeValueProviderProviderNameTestCase(IsolatedAsyncioTestCase):
    """Tests for provider_name propagation."""

    async def test_provider_name_propagates_to_nested(self):
        """provider_name should propagate to all nested providers."""
        distributor = StubDistributor(raise_cursor_at=0)
        provider = ResumeIdProvider(distributor)

        provider.provider_name = 'root'

        self.assertEqual(provider.provider_name, 'root')
        self.assertEqual(provider.user_id.provider_name, 'root.user_id')
        self.assertEqual(provider.user_id.tenant_id.provider_name, 'root.user_id.tenant_id')
        self.assertEqual(provider.user_id.internal_user_id.provider_name, 'root.user_id.internal_user_id')
        self.assertEqual(provider.internal_resume_id.provider_name, 'root.internal_resume_id')


if __name__ == '__main__':
    import unittest
    unittest.main()
