import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.composite_value_provider import CompositeValueProvider
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty


# =============================================================================
# Value Objects - Level 1 (Simple)
# =============================================================================

@dataclasses.dataclass(frozen=True)
class TenantId:
    """Simple value object for tenant ID."""
    value: int


@dataclasses.dataclass(frozen=True)
class InternalUserId:
    """Simple value object for internal user ID within tenant."""
    value: int


@dataclasses.dataclass(frozen=True)
class InternalResumeId:
    """Simple value object for internal resume ID within user."""
    value: int


# =============================================================================
# Value Objects - Level 2 (Composite)
# =============================================================================

@dataclasses.dataclass(frozen=True)
class UserId:
    """Composite ID: TenantId + InternalUserId."""
    tenant_id: TenantId
    internal_user_id: InternalUserId


@dataclasses.dataclass(frozen=True)
class ResumeId:
    """Composite ID: UserId + InternalResumeId."""
    user_id: UserId
    internal_resume_id: InternalResumeId


# =============================================================================
# Aggregates
# =============================================================================

@dataclasses.dataclass
class Tenant:
    """Tenant aggregate - root level, no references."""
    id: TenantId
    name: str


@dataclasses.dataclass
class User:
    """User aggregate - references Tenant via TenantId FK."""
    id: UserId
    tenant_id: TenantId  # FK reference to Tenant aggregate
    username: str


@dataclasses.dataclass
class Resume:
    """Resume aggregate - references User via UserId FK."""
    id: ResumeId
    user_id: UserId  # FK reference to User aggregate
    title: str


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


# =============================================================================
# Stub Repositories
# =============================================================================

class StubTenantRepository(IAggregateRepository[Tenant]):
    """Stub repository for Tenant aggregates."""

    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[int, Tenant] = {}
        self._auto_increment_counter = auto_increment_start
        self._inserted: list[Tenant] = []

    # IObservable methods
    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    async def insert(self, session: ISession, agg: Tenant):
        if agg.id is None or (isinstance(agg.id, TenantId) and agg.id.value == 0):
            new_id = TenantId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = new_id
        self._storage[agg.id.value] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: Tenant):
        self._storage[agg.id.value] = agg

    async def get(self, session: ISession, id_: TenantId) -> Tenant | None:
        if isinstance(id_, TenantId):
            return self._storage.get(id_.value)
        return self._storage.get(id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[Tenant]:
        return list(self._storage.values())

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


class StubUserRepository(IAggregateRepository[User]):
    """Stub repository for User aggregates."""

    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[tuple[int, int], User] = {}
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
        if agg.id.internal_user_id is None or (
                isinstance(agg.id.internal_user_id, InternalUserId) and agg.id.internal_user_id.value == 0
        ):
            new_internal_id = InternalUserId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = UserId(tenant_id=agg.id.tenant_id, internal_user_id=new_internal_id)
        key = (agg.id.tenant_id.value, agg.id.internal_user_id.value)
        self._storage[key] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: User):
        key = (agg.id.tenant_id.value, agg.id.internal_user_id.value)
        self._storage[key] = agg

    async def get(self, session: ISession, id_: UserId) -> User | None:
        if isinstance(id_, UserId):
            key = (id_.tenant_id.value, id_.internal_user_id.value)
            return self._storage.get(key)
        return None

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[User]:
        return list(self._storage.values())

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


class StubResumeRepository(IAggregateRepository[Resume]):
    """Stub repository for Resume aggregates."""

    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[tuple[int, int, int], Resume] = {}
        self._auto_increment_counter = auto_increment_start
        self._inserted: list[Resume] = []

    # IObservable methods
    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    async def insert(self, session: ISession, agg: Resume):
        if agg.id.internal_resume_id is None or (
                isinstance(agg.id.internal_resume_id, InternalResumeId) and agg.id.internal_resume_id.value == 0
        ):
            new_internal_id = InternalResumeId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = ResumeId(user_id=agg.id.user_id, internal_resume_id=new_internal_id)
        key = (
            agg.id.user_id.tenant_id.value,
            agg.id.user_id.internal_user_id.value,
            agg.id.internal_resume_id.value
        )
        self._storage[key] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: Resume):
        key = (
            agg.id.user_id.tenant_id.value,
            agg.id.user_id.internal_user_id.value,
            agg.id.internal_resume_id.value
        )
        self._storage[key] = agg

    async def get(self, session: ISession, id_: ResumeId) -> Resume | None:
        if isinstance(id_, ResumeId):
            key = (
                id_.user_id.tenant_id.value,
                id_.user_id.internal_user_id.value,
                id_.internal_resume_id.value
            )
            return self._storage.get(key)
        return None

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[Resume]:
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

async def tenant_id_generator(session: ISession, position: int | None = None) -> int:
    return (position if position is not None else 0) + 100


async def internal_user_id_generator(session: ISession, position: int | None = None) -> int:
    return (position if position is not None else 0) + 200


async def internal_resume_id_generator(session: ISession, position: int | None = None) -> int:
    return (position if position is not None else 0) + 300


async def tenant_name_generator(session: ISession, position: int | None = None) -> str:
    return f"Tenant_{position if position is not None else 0}"


async def username_generator(session: ISession, position: int | None = None) -> str:
    return f"user_{position if position is not None else 0}"


async def resume_title_generator(session: ISession, position: int | None = None) -> str:
    return f"Resume_{position if position is not None else 0}"


# =============================================================================
# Composite ID Providers
# =============================================================================

class UserIdProvider(CompositeValueProvider[dict, UserId]):
    """Provider for UserId composite."""
    tenant_id: ValueProvider[int, TenantId]
    internal_user_id: ValueProvider[int, InternalUserId]

    def __init__(self, tenant_id_distributor: IM2ODistributor, internal_user_id_distributor: IM2ODistributor):
        self.tenant_id = ValueProvider(
            distributor=tenant_id_distributor,
            value_generator=tenant_id_generator,
            result_factory=TenantId,
            result_exporter=lambda x: x.value,
        )
        self.internal_user_id = ValueProvider(
            distributor=internal_user_id_distributor,
            value_generator=internal_user_id_generator,
            result_factory=InternalUserId,
            result_exporter=lambda x: x.value,
        )
        super().__init__(
            distributor=StubDistributor(raise_cursor_at=0),
            result_factory=UserId,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(user_id: UserId) -> dict:
        return {
            'tenant_id': user_id.tenant_id.value,
            'internal_user_id': user_id.internal_user_id.value,
        }


class ResumeIdProvider(CompositeValueProvider[dict, ResumeId]):
    """Provider for ResumeId composite."""
    user_id: UserIdProvider
    internal_resume_id: ValueProvider[int, InternalResumeId]

    def __init__(
            self,
            tenant_id_distributor: IM2ODistributor,
            internal_user_id_distributor: IM2ODistributor,
            internal_resume_id_distributor: IM2ODistributor
    ):
        self.user_id = UserIdProvider(tenant_id_distributor, internal_user_id_distributor)
        self.internal_resume_id = ValueProvider(
            distributor=internal_resume_id_distributor,
            value_generator=internal_resume_id_generator,
            result_factory=InternalResumeId,
            result_exporter=lambda x: x.value,
        )
        super().__init__(
            distributor=StubDistributor(raise_cursor_at=0),
            result_factory=ResumeId,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(resume_id: ResumeId) -> dict:
        return {
            'user_id': UserIdProvider._export(resume_id.user_id),
            'internal_resume_id': resume_id.internal_resume_id.value,
        }


# =============================================================================
# Aggregate Providers - Auto Increment PK
# =============================================================================

class TenantProviderAutoIncrement(AggregateProvider[dict, Tenant]):
    """Tenant provider with auto-increment ID."""
    _id_attr = 'id'

    id: ValueProvider[int, TenantId]
    name: ValueProvider[str, str]

    def __init__(self, repository: IAggregateRepository[Tenant]):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=lambda: 0,
            result_factory=TenantId,
            result_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=tenant_name_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=Tenant,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(tenant: Tenant) -> dict:
        return {'id': tenant.id.value, 'name': tenant.name}


class UserProviderAutoIncrement(AggregateProvider[dict, User]):
    """User provider with auto-increment ID and reference to Tenant."""
    _id_attr = 'id'

    id: UserIdProvider
    tenant_id: ReferenceProvider
    username: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository[User],
            tenant_provider: TenantProviderAutoIncrement
    ):
        # ID with auto-increment for internal_user_id
        self.id = UserIdProvider(
            tenant_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_user_id_distributor=StubDistributor(raise_cursor_at=0),
        )
        # Reference to Tenant
        self.tenant_id = ReferenceProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            aggregate_provider=tenant_provider,
        )
        self.username = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=username_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=User,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': UserIdProvider._export(user.id),
            'tenant_id': user.tenant_id.value,
            'username': user.username,
        }


class ResumeProviderAutoIncrement(AggregateProvider[dict, Resume]):
    """Resume provider with auto-increment ID and reference to User."""
    _id_attr = 'id'

    id: ResumeIdProvider
    user_id: ReferenceProvider
    title: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository[Resume],
            user_provider: UserProviderAutoIncrement
    ):
        self.id = ResumeIdProvider(
            tenant_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_user_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_resume_id_distributor=StubDistributor(raise_cursor_at=0),
        )
        self.user_id = ReferenceProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            aggregate_provider=user_provider,
        )
        self.title = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=resume_title_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=Resume,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(resume: Resume) -> dict:
        return {
            'id': ResumeIdProvider._export(resume.id),
            'user_id': UserIdProvider._export(resume.user_id),
            'title': resume.title,
        }


# =============================================================================
# Aggregate Providers - Pre-set PK
# =============================================================================

class TenantProviderPresetPK(AggregateProvider[dict, Tenant]):
    """Tenant provider with pre-set ID from generator."""
    _id_attr = 'id'

    id: ValueProvider[int, TenantId]
    name: ValueProvider[str, str]

    def __init__(self, repository: IAggregateRepository[Tenant]):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=tenant_id_generator,
            result_factory=TenantId,
            result_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=tenant_name_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=Tenant,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(tenant: Tenant) -> dict:
        return {'id': tenant.id.value, 'name': tenant.name}


class UserProviderPresetPK(AggregateProvider[dict, User]):
    """User provider with pre-set ID and reference to Tenant."""
    _id_attr = 'id'

    id: UserIdProvider
    tenant_id: ReferenceProvider
    username: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository[User],
            tenant_provider: TenantProviderPresetPK
    ):
        self.id = UserIdProvider(
            tenant_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_user_id_distributor=StubDistributor(raise_cursor_at=0),
        )
        self.tenant_id = ReferenceProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            aggregate_provider=tenant_provider,
        )
        self.username = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=username_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=User,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': UserIdProvider._export(user.id),
            'tenant_id': user.tenant_id.value,
            'username': user.username,
        }


class ResumeProviderPresetPK(AggregateProvider[dict, Resume]):
    """Resume provider with pre-set ID and reference to User."""
    _id_attr = 'id'

    id: ResumeIdProvider
    user_id: ReferenceProvider
    title: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository[Resume],
            user_provider: UserProviderPresetPK
    ):
        self.id = ResumeIdProvider(
            tenant_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_user_id_distributor=StubDistributor(raise_cursor_at=0),
            internal_resume_id_distributor=StubDistributor(raise_cursor_at=0),
        )
        self.user_id = ReferenceProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            aggregate_provider=user_provider,
        )
        self.title = ValueProvider(
            distributor=StubDistributor(raise_cursor_at=0),
            value_generator=resume_title_generator,
        )
        super().__init__(
            repository=repository,
            result_factory=Resume,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(resume: Resume) -> dict:
        return {
            'id': ResumeIdProvider._export(resume.id),
            'user_id': UserIdProvider._export(resume.user_id),
            'title': resume.title,
        }


# =============================================================================
# Test Cases: Auto-increment PK - Basic Reference
# =============================================================================

class ReferenceProviderAutoIncrementBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for ReferenceProvider with auto-increment PK."""

    async def test_reference_creates_referenced_aggregate(self):
        """ReferenceProvider should create the referenced aggregate."""
        tenant_repo = StubTenantRepository(auto_increment_start=1)
        user_repo = StubUserRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)
        user = await user_provider.create(session)

        self.assertIsInstance(user, User)
        self.assertIsInstance(user.tenant_id, TenantId)
        self.assertEqual(len(tenant_repo._inserted), 1)

    async def test_reference_provider_creates_aggregate_with_auto_increment_id(self):
        """ReferenceProvider creates aggregate which gets auto-increment ID from repository."""
        tenant_repo = StubTenantRepository(auto_increment_start=10)
        user_repo = StubUserRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)
        await user_provider.create(session)

        # The actual Tenant aggregate gets auto-increment ID from repository
        tenant = user_provider.tenant_id.aggregate_provider._output_result
        self.assertEqual(tenant.id.value, 10)
        # Repository stored the aggregate with correct ID
        self.assertEqual(len(tenant_repo._inserted), 1)
        self.assertEqual(tenant_repo._inserted[0].id.value, 10)

    async def test_tenant_aggregate_gets_auto_increment_id(self):
        """The Tenant aggregate gets auto-incremented ID from repository."""
        tenant_repo = StubTenantRepository(auto_increment_start=5)
        user_repo = StubUserRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)
        await user_provider.create(session)

        # The referenced Tenant aggregate has the auto-incremented ID
        tenant = user_provider.tenant_id.aggregate_provider._output_result
        self.assertEqual(tenant.id.value, 5)
        # Verify repository assigned the ID correctly
        self.assertEqual(tenant_repo._inserted[0].id.value, 5)

    async def test_is_complete_after_populate_with_cursor(self):
        """is_complete() should return True after populate() when ICursor is raised."""
        tenant_repo = StubTenantRepository(auto_increment_start=1)
        user_repo = StubUserRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)

        # Key assertion: is_complete() must be True after populate()
        # This tests the ICursor branch where _output_result must be set AFTER set()
        self.assertTrue(user_provider.tenant_id.is_complete())
        self.assertIsNotNone(user_provider.tenant_id._output_result)


# =============================================================================
# Test Cases: Auto-increment PK - Multi-level Reference
# =============================================================================

class ReferenceProviderAutoIncrementMultiLevelTestCase(IsolatedAsyncioTestCase):
    """Tests for multi-level references with auto-increment PK."""

    async def test_three_level_hierarchy_creation(self):
        """Resume -> User -> Tenant hierarchy should be created correctly."""
        tenant_repo = StubTenantRepository(auto_increment_start=1)
        user_repo = StubUserRepository(auto_increment_start=1)
        resume_repo = StubResumeRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        resume_provider = ResumeProviderAutoIncrement(resume_repo, user_provider)
        resume_provider.provider_name = 'resume'

        await resume_provider.populate(session)
        resume = await resume_provider.create(session)

        # Verify structure
        self.assertIsInstance(resume, Resume)
        self.assertIsInstance(resume.user_id, UserId)

        # Verify all aggregates were inserted
        self.assertEqual(len(tenant_repo._inserted), 1)
        self.assertEqual(len(user_repo._inserted), 1)
        self.assertEqual(len(resume_repo._inserted), 1)

    async def test_all_aggregates_inserted_in_repositories(self):
        """All aggregates in hierarchy are inserted into their repositories."""
        tenant_repo = StubTenantRepository(auto_increment_start=10)
        user_repo = StubUserRepository(auto_increment_start=20)
        resume_repo = StubResumeRepository(auto_increment_start=30)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        resume_provider = ResumeProviderAutoIncrement(resume_repo, user_provider)
        resume_provider.provider_name = 'resume'

        await resume_provider.populate(session)
        resume = await resume_provider.create(session)

        # Verify all aggregates were inserted in repositories
        self.assertEqual(len(tenant_repo._inserted), 1)
        self.assertEqual(len(user_repo._inserted), 1)
        self.assertEqual(len(resume_repo._inserted), 1)

        # Verify Tenant gets auto-increment ID
        tenant = tenant_repo._inserted[0]
        self.assertEqual(tenant.id.value, 10)

        # Resume's internal_resume_id comes from generator (300), not auto-increment
        # because the generator returns non-zero value
        self.assertEqual(resume.id.internal_resume_id.value, 300)

    async def test_multiple_resumes_share_same_user(self):
        """Multiple resumes can reference the same user."""
        tenant_repo = StubTenantRepository(auto_increment_start=1)
        user_repo = StubUserRepository(auto_increment_start=1)
        resume_repo = StubResumeRepository(auto_increment_start=1)
        session = MockSession()

        tenant_provider = TenantProviderAutoIncrement(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderAutoIncrement(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        # Create first user and resume
        await user_provider.populate(session)
        user1 = await user_provider.create(session)

        resume_provider1 = ResumeProviderAutoIncrement(resume_repo, user_provider)
        resume_provider1.provider_name = 'resume1'

        # Pre-populate with existing user reference
        resume_provider1.user_id._distributor._values.append(user1)
        resume_provider1.user_id._distributor._raise_cursor_at = None

        await resume_provider1.populate(session)
        resume1 = await resume_provider1.create(session)

        # Both should reference same user
        self.assertEqual(resume1.user_id, user1.id)
        # Only one user should be inserted
        self.assertEqual(len(user_repo._inserted), 1)


# =============================================================================
# Test Cases: Pre-set PK - Basic Reference
# =============================================================================

class ReferenceProviderPresetPKBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for ReferenceProvider with pre-set PK."""

    async def test_reference_with_preset_id(self):
        """ReferenceProvider should work with pre-set IDs."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)
        user = await user_provider.create(session)

        self.assertIsInstance(user, User)
        # Preset ID from generator: 100 + position(0)
        self.assertEqual(user.tenant_id.value, 100)

    async def test_preset_ids_propagate_through_hierarchy(self):
        """Pre-set IDs should propagate through the entire hierarchy."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        resume_repo = StubResumeRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        resume_provider = ResumeProviderPresetPK(resume_repo, user_provider)
        resume_provider.provider_name = 'resume'

        await resume_provider.populate(session)
        resume = await resume_provider.create(session)

        # Verify preset IDs from generators
        self.assertEqual(resume.user_id.tenant_id.value, 100)  # tenant_id_generator
        self.assertEqual(resume.user_id.internal_user_id.value, 200)  # internal_user_id_generator
        self.assertEqual(resume.id.internal_resume_id.value, 300)  # internal_resume_id_generator

    async def test_reuse_existing_referenced_aggregate(self):
        """ReferenceProvider should reuse existing referenced aggregate."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        # Create tenant first
        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'
        await tenant_provider.populate(session)
        existing_tenant = await tenant_provider.create(session)

        # Create user with reference to existing tenant
        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        # Pre-populate distributor with existing tenant
        user_provider.tenant_id._distributor._values.append(existing_tenant)
        user_provider.tenant_id._distributor._raise_cursor_at = None

        await user_provider.populate(session)
        user = await user_provider.create(session)

        # Should reuse existing tenant
        self.assertEqual(user.tenant_id, existing_tenant.id)
        self.assertEqual(len(tenant_repo._inserted), 1)


# =============================================================================
# Test Cases: Pre-set PK - Multi-level Reference
# =============================================================================

class ReferenceProviderPresetPKMultiLevelTestCase(IsolatedAsyncioTestCase):
    """Tests for multi-level references with pre-set PK."""

    async def test_full_hierarchy_with_preset_ids(self):
        """Full Resume -> User -> Tenant hierarchy with preset IDs."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        resume_repo = StubResumeRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        resume_provider = ResumeProviderPresetPK(resume_repo, user_provider)
        resume_provider.provider_name = 'resume'

        await resume_provider.populate(session)
        resume = await resume_provider.create(session)

        # Verify complete structure
        self.assertIsInstance(resume, Resume)
        self.assertIsInstance(resume.user_id, UserId)

        # All were inserted
        self.assertEqual(len(tenant_repo._inserted), 1)
        self.assertEqual(len(user_repo._inserted), 1)
        self.assertEqual(len(resume_repo._inserted), 1)

    async def test_set_propagates_to_referenced_aggregates(self):
        """set() should propagate values to referenced aggregate providers."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        # Set values manually
        user_provider.set({
            'id': {
                'tenant_id': 999,
                'internal_user_id': 888,
            },
            'tenant_id': 999,
            'username': 'custom_user',
        })

        self.assertEqual(user_provider.id.tenant_id.get(), 999)
        self.assertEqual(user_provider.id.internal_user_id.get(), 888)
        self.assertEqual(user_provider.username.get(), 'custom_user')


# =============================================================================
# Test Cases: Provider Name Propagation
# =============================================================================

class ReferenceProviderNameTestCase(IsolatedAsyncioTestCase):
    """Tests for provider_name propagation in ReferenceProvider."""

    async def test_provider_name_propagates_to_reference(self):
        """provider_name should propagate to ReferenceProvider."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        self.assertEqual(user_provider.tenant_id.provider_name, 'user.tenant_id')

    async def test_nested_provider_names_in_hierarchy(self):
        """Provider names should propagate correctly through hierarchy."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        resume_repo = StubResumeRepository()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        resume_provider = ResumeProviderPresetPK(resume_repo, user_provider)
        resume_provider.provider_name = 'resume'

        self.assertEqual(resume_provider.user_id.provider_name, 'resume.user_id')
        self.assertEqual(resume_provider.id.provider_name, 'resume.id')
        self.assertEqual(resume_provider.id.user_id.provider_name, 'resume.id.user_id')


# =============================================================================
# Test Cases: Reset
# =============================================================================

class ReferenceProviderResetTestCase(IsolatedAsyncioTestCase):
    """Tests for reset functionality in ReferenceProvider."""

    async def test_reset_clears_reference_provider(self):
        """reset() should clear ReferenceProvider state."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        await user_provider.populate(session)
        self.assertTrue(user_provider.tenant_id.is_complete())

        user_provider.reset()

        self.assertFalse(user_provider.tenant_id.is_complete())
        self.assertEqual(user_provider.tenant_id._input_value, empty)


# =============================================================================
# Test Cases: Setup and Cleanup
# =============================================================================

class ReferenceProviderSetupCleanupTestCase(IsolatedAsyncioTestCase):
    """Tests for setup and cleanup in ReferenceProvider."""

    async def test_setup_propagates_to_referenced_provider(self):
        """setup() should propagate to referenced aggregate provider."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        # Should not raise
        await user_provider.setup(session)

    async def test_cleanup_propagates_to_referenced_provider(self):
        """cleanup() should propagate to referenced aggregate provider."""
        tenant_repo = StubTenantRepository()
        user_repo = StubUserRepository()
        session = MockSession()

        tenant_provider = TenantProviderPresetPK(tenant_repo)
        tenant_provider.provider_name = 'tenant'

        user_provider = UserProviderPresetPK(user_repo, tenant_provider)
        user_provider.provider_name = 'user'

        # Should not raise
        await user_provider.cleanup(session)


if __name__ == '__main__':
    import unittest
    unittest.main()
