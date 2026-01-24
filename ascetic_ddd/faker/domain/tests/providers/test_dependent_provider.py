import dataclasses
import typing
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.dependent_provider import DependentProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty


# =============================================================================
# Value Objects and Aggregates for testing
# =============================================================================

@dataclasses.dataclass(frozen=True)
class EmployeeId:
    """Simple value object for employee ID."""
    value: int


@dataclasses.dataclass
class Employee:
    """Dependent aggregate (child in 1:M relationship)."""
    id: EmployeeId
    name: str
    company_id: int


# =============================================================================
# Stub O2M Distributor
# =============================================================================

class StubO2MDistributor(IO2MDistributor):
    """Stub O2M distributor that returns fixed count."""

    def __init__(self, count: int):
        self._count = count

    def distribute(self) -> int:
        return self._count


class SequentialO2MDistributor(IO2MDistributor):
    """O2M distributor that returns values from a sequence."""

    def __init__(self, counts: list[int]):
        self._counts = counts
        self._index = 0

    def distribute(self) -> int:
        if self._index < len(self._counts):
            count = self._counts[self._index]
            self._index += 1
            return count
        return 0


# =============================================================================
# Stub M2O Distributor (for ValueProvider)
# =============================================================================

class StubM2ODistributor(IM2ODistributor):
    """Stub distributor that always raises Cursor."""

    def __init__(self):
        self._provider_name = None
        self._appended = []

    async def next(self, session: ISession, specification=None):
        raise Cursor(position=len(self._appended), callback=self._append)

    async def _append(self, session: ISession, value, position: int | None):
        self._appended.append((value, position))

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
        return StubM2ODistributor()

    def __deepcopy__(self, memodict={}):
        return StubM2ODistributor()

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

class StubEmployeeRepository(IAggregateRepository[Employee]):
    """Stub repository for testing."""

    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[int, Employee] = {}
        self._auto_increment_counter = auto_increment_start
        self._inserted: list[Employee] = []

    # IObservable methods
    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    async def insert(self, session: ISession, agg: Employee):
        if agg.id is None or (isinstance(agg.id, EmployeeId) and agg.id.value == 0):
            new_id = EmployeeId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = new_id
        self._storage[agg.id.value] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: Employee):
        self._storage[agg.id.value] = agg

    async def get(self, session: ISession, id_: EmployeeId) -> Employee | None:
        if isinstance(id_, EmployeeId):
            return self._storage.get(id_.value)
        return self._storage.get(id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[Employee]:
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

_name_counter = 0


async def name_generator(session: ISession, position: int | None = None) -> str:
    global _name_counter
    _name_counter += 1
    return f"Employee_{_name_counter}"


async def company_id_generator(session: ISession, position: int | None = None) -> int:
    return 1  # Fixed company_id for testing


# =============================================================================
# Employee Provider (child in 1:M)
# =============================================================================

class EmployeeProvider(AggregateProvider[dict, Employee]):
    """Provider for Employee - child in 1:M relationship."""
    _id_attr = 'id'

    id: ValueProvider[int, EmployeeId]
    name: ValueProvider[str, str]
    company_id: ValueProvider[int, int]

    def __init__(self, repository: IAggregateRepository[Employee]):
        self.id = ValueProvider(
            distributor=StubM2ODistributor(),
            input_generator=lambda: 0,
            output_factory=EmployeeId,
            output_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubM2ODistributor(),
            input_generator=name_generator,
        )
        self.company_id = ValueProvider(
            distributor=StubM2ODistributor(),
            input_generator=company_id_generator,
        )
        super().__init__(
            repository=repository,
            output_factory=Employee,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(employee: Employee) -> dict:
        return {
            'id': employee.id.value,
            'name': employee.name,
            'company_id': employee.company_id,
        }


# =============================================================================
# Test Cases: Basic Functionality
# =============================================================================

class DependentProviderBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for DependentProvider."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_populate_creates_children(self):
        """populate() should create N children based on distributor count."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=3)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(len(provider._outputs), 3)
        self.assertEqual(len(repository._inserted), 3)

    async def test_create_returns_list_of_ids(self):
        """create() should return list of child IDs."""
        repository = StubEmployeeRepository(auto_increment_start=10)
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)
        ids = await provider.create(session)

        self.assertEqual(len(ids), 2)
        self.assertEqual(ids[0].value, 10)
        self.assertEqual(ids[1].value, 11)

    async def test_zero_count_creates_no_children(self):
        """When distributor returns 0, no children should be created."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=0)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(len(provider._outputs), 0)
        self.assertEqual(len(repository._inserted), 0)

    async def test_is_complete_false_before_populate(self):
        """is_complete() should return False before populate()."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )

        self.assertFalse(provider.is_complete())

    async def test_is_complete_true_after_populate(self):
        """is_complete() should return True after populate()."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())


# =============================================================================
# Test Cases: Pre-set Values
# =============================================================================

class DependentProviderSetGetTestCase(IsolatedAsyncioTestCase):
    """Tests for set() and get() methods."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_set_determines_count(self):
        """set() should determine count from values list length."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=5)  # Should be ignored

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # Pre-set 2 values (should override distributor's count of 5)
        provider.set([
            {'name': 'John', 'company_id': 1},
            {'name': 'Jane', 'company_id': 1},
        ])

        await provider.populate(session)

        self.assertEqual(len(provider._outputs), 2)
        self.assertEqual(provider._outputs[0].name, 'John')
        self.assertEqual(provider._outputs[1].name, 'Jane')

    async def test_get_returns_values_from_providers(self):
        """get() should return list of values from child providers."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)
        values = provider.get()

        self.assertEqual(len(values), 2)
        self.assertIn('name', values[0])
        self.assertIn('company_id', values[0])


# =============================================================================
# Test Cases: Reset
# =============================================================================

class DependentProviderResetTestCase(IsolatedAsyncioTestCase):
    """Tests for reset functionality."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_reset_clears_state(self):
        """reset() should clear provider state."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)
        self.assertTrue(provider.is_complete())

        provider.reset()

        self.assertFalse(provider.is_complete())
        self.assertEqual(provider._inputs, empty)
        self.assertEqual(provider._outputs, empty)
        self.assertIsNone(provider._count)

    async def test_reset_allows_repopulate(self):
        """After reset(), populate() should work again."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = SequentialO2MDistributor(counts=[2, 3])

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # First populate
        await provider.populate(session)
        self.assertEqual(len(provider._outputs), 2)

        # Reset and repopulate
        provider.reset()
        await provider.populate(session)

        self.assertEqual(len(provider._outputs), 3)


# =============================================================================
# Test Cases: Lazy Provider Factory
# =============================================================================

class DependentProviderLazyTestCase(IsolatedAsyncioTestCase):
    """Tests for lazy aggregate provider resolution."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_lazy_provider_resolution(self):
        """DependentProvider should support lazy provider factory."""
        repository = StubEmployeeRepository()
        distributor = StubO2MDistributor(count=2)

        # Use factory instead of direct provider
        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=lambda: EmployeeProvider(repository),
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(len(provider._outputs), 2)


# =============================================================================
# Test Cases: Transient State
# =============================================================================

class DependentProviderTransientTestCase(IsolatedAsyncioTestCase):
    """Tests for is_transient() method."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_is_transient_true_before_set(self):
        """is_transient() should return True when no values set."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )

        self.assertTrue(provider.is_transient())

    async def test_is_transient_false_after_set(self):
        """is_transient() should return False after set()."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )

        provider.set([{'name': 'Test', 'company_id': 1}])

        self.assertFalse(provider.is_transient())


# =============================================================================
# Test Cases: Cloning (empty)
# =============================================================================

class DependentProviderCloneTestCase(IsolatedAsyncioTestCase):
    """Tests for empty() cloning method."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_empty_creates_clean_clone(self):
        """empty() should create a clean clone."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=2)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        await provider.populate(session)
        self.assertTrue(provider.is_complete())

        clone = provider.empty()

        self.assertFalse(clone.is_complete())
        self.assertEqual(clone._inputs, empty)
        self.assertEqual(clone._outputs, empty)


# =============================================================================
# Test Cases: DependentProvider as AggregateProvider member
# =============================================================================

@dataclasses.dataclass(frozen=True)
class CompanyId:
    value: int


@dataclasses.dataclass
class Company:
    id: CompanyId
    name: str
    # Note: no 'employees' field - it's a 1:M relationship, not aggregate field


class StubCompanyRepository(IAggregateRepository[Company]):
    def __init__(self, auto_increment_start: int = 1):
        self._storage: dict[int, Company] = {}
        self._auto_increment_counter = auto_increment_start
        self._inserted: list[Company] = []

    # IObservable methods
    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    async def insert(self, session: ISession, agg: Company):
        if agg.id is None or (isinstance(agg.id, CompanyId) and agg.id.value == 0):
            new_id = CompanyId(value=self._auto_increment_counter)
            self._auto_increment_counter += 1
            agg.id = new_id
        self._storage[agg.id.value] = agg
        self._inserted.append(agg)

    async def update(self, session: ISession, agg: Company):
        self._storage[agg.id.value] = agg

    async def get(self, session: ISession, id_: CompanyId) -> Company | None:
        if isinstance(id_, CompanyId):
            return self._storage.get(id_.value)
        return self._storage.get(id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[Company]:
        return list(self._storage.values())

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


async def company_name_generator(session: ISession, position: int | None = None) -> str:
    return f"Company_{position if position is not None else 0}"


class CompanyProviderWithEmployees(AggregateProvider[dict, Company]):
    """
    Company provider that includes DependentProvider for employees.
    This tests integration of DependentProvider within AggregateProvider.
    """
    _id_attr = 'id'

    id: ValueProvider[int, CompanyId]
    name: ValueProvider[str, str]
    employees: DependentProvider[dict, Employee, EmployeeId]

    def __init__(
            self,
            repository: IAggregateRepository[Company],
            employee_repository: IAggregateRepository[Employee],
            employee_distributor: IO2MDistributor,
    ):
        self.id = ValueProvider(
            distributor=StubM2ODistributor(),
            input_generator=lambda: 0,
            output_factory=CompanyId,
            output_exporter=lambda x: x.value,
        )
        self.name = ValueProvider(
            distributor=StubM2ODistributor(),
            input_generator=company_name_generator,
        )
        # DependentProvider for 1:M relationship
        self.employees = DependentProvider(
            distributor=employee_distributor,
            aggregate_provider=EmployeeProvider(employee_repository),
            dependency_field='company_id',  # FK field in Employee
        )
        super().__init__(
            repository=repository,
            output_factory=Company,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(company: Company) -> dict:
        return {
            'id': company.id.value,
            'name': company.name,
        }


class DependentProviderAsAggregateMemberTestCase(IsolatedAsyncioTestCase):
    """Tests for DependentProvider as a member of AggregateProvider."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_dependent_provider_in_aggregate_provider(self):
        """
        DependentProvider should work when it's a member of AggregateProvider.

        Problem: AggregateProvider._default_factory() iterates over _providers
        and calls create() on each, but:
        1. DependentProvider.create() returns list, not a single value
        2. Company dataclass doesn't have 'employees' field
        3. employees need company_id FK, but company might not have ID yet
        """
        company_repo = StubCompanyRepository(auto_increment_start=42)  # Not 1!
        employee_repo = StubEmployeeRepository()
        employee_distributor = StubO2MDistributor(count=3)

        provider = CompanyProviderWithEmployees(
            repository=company_repo,
            employee_repository=employee_repo,
            employee_distributor=employee_distributor,
        )
        provider.provider_name = 'company'
        session = MockSession()

        # This should create a company with 3 employees
        await provider.populate(session)
        company = await provider.create(session)

        # Verify company was created
        self.assertIsInstance(company, Company)
        self.assertEqual(len(company_repo._inserted), 1)

        # Verify employees were created
        self.assertEqual(len(employee_repo._inserted), 3)

        # Verify employees' company_id points to the created company
        for employee in employee_repo._inserted:
            self.assertEqual(employee.company_id, company.id.value)


# =============================================================================
# Test Cases: Weighted Selection
# =============================================================================

class DependentProviderWeightedTestCase(IsolatedAsyncioTestCase):
    """Tests for weighted value selection mode."""

    def setUp(self):
        global _name_counter
        _name_counter = 0

    async def test_weighted_mode_creates_count_from_distributor(self):
        """In weighted mode, count should come from distributor, not values length."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=100)  # Create 100 children

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # Only 2 template values, but 100 children should be created
        provider.set(
            [{'name': 'IT Employee', 'company_id': 1}, {'name': 'HR Employee', 'company_id': 1}],
            weights=[0.7, 0.3]
        )

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(len(provider._outputs), 100)
        self.assertEqual(len(repository._inserted), 100)

    async def test_weighted_mode_distribution(self):
        """Weighted mode should distribute values according to weights."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=1000)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # 90% should get 'IT', 10% should get 'HR'
        provider.set(
            [{'name': 'IT', 'company_id': 1}, {'name': 'HR', 'company_id': 1}],
            weights=[0.9, 0.1]
        )

        await provider.populate(session)

        # Count distribution
        it_count = sum(1 for e in provider._outputs if e.name == 'IT')
        hr_count = sum(1 for e in provider._outputs if e.name == 'HR')

        # With 1000 samples and 90/10 split, IT should be roughly 900 (±100)
        self.assertGreater(it_count, 700)
        self.assertLess(it_count, 990)
        self.assertEqual(it_count + hr_count, 1000)

    async def test_weighted_mode_with_multiple_values(self):
        """Weighted mode should work with more than 2 values."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=500)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # 4 departments with different weights
        provider.set(
            [
                {'name': 'Engineering', 'company_id': 1},
                {'name': 'Sales', 'company_id': 1},
                {'name': 'Marketing', 'company_id': 1},
                {'name': 'HR', 'company_id': 1},
            ],
            weights=[0.5, 0.25, 0.15, 0.1]
        )

        await provider.populate(session)

        self.assertEqual(len(provider._outputs), 500)

        # Check that all departments are represented
        names = {e.name for e in provider._outputs}
        self.assertEqual(names, {'Engineering', 'Sales', 'Marketing', 'HR'})

    async def test_weighted_mode_reset_clears_selector(self):
        """reset() should clear weighted mode state."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=10)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'

        provider.set(
            [{'name': 'A', 'company_id': 1}, {'name': 'B', 'company_id': 1}],
            weights=[0.5, 0.5]
        )

        self.assertIsNotNone(provider._value_selector)
        self.assertIsNotNone(provider._weights)

        provider.reset()

        self.assertIsNone(provider._value_selector)
        self.assertIsNone(provider._weights)
        self.assertEqual(provider._inputs, empty)

    async def test_switch_from_weighted_to_direct_mode(self):
        """Calling set() without weights should switch to direct mode."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=100)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'
        session = MockSession()

        # First, set with weights
        provider.set(
            [{'name': 'A', 'company_id': 1}],
            weights=[1.0]
        )
        self.assertIsNotNone(provider._value_selector)

        # Now switch to direct mode
        provider.set([{'name': 'Direct1', 'company_id': 1}, {'name': 'Direct2', 'company_id': 1}])
        self.assertIsNone(provider._value_selector)
        self.assertEqual(provider._count, 2)

        await provider.populate(session)

        self.assertEqual(len(provider._outputs), 2)

    async def test_empty_preserves_weighted_mode_reset(self):
        """empty() clone should not have weighted mode state."""
        repository = StubEmployeeRepository()
        template = EmployeeProvider(repository)
        distributor = StubO2MDistributor(count=10)

        provider = DependentProvider(
            distributor=distributor,
            aggregate_provider=template,
        )
        provider.provider_name = 'employees'

        provider.set(
            [{'name': 'A', 'company_id': 1}],
            weights=[1.0]
        )

        clone = provider.empty()

        self.assertIsNone(clone._value_selector)
        self.assertIsNone(clone._weights)
        self.assertEqual(clone._inputs, empty)


if __name__ == '__main__':
    import unittest
    unittest.main()
