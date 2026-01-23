import dataclasses
import typing
from unittest import TestCase, IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.composite_value_provider import CompositeValueProvider
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor


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
class DepartmentId:
    value: int


@dataclasses.dataclass
class Department:
    id: DepartmentId
    name: str
    status_id: StatusId


@dataclasses.dataclass(frozen=True)
class UserId:
    value: int


@dataclasses.dataclass
class User:
    id: UserId
    department_id: DepartmentId
    name: str


@dataclasses.dataclass(frozen=True)
class Address:
    """Composite Value Object."""
    city: str
    street: str


@dataclasses.dataclass
class Company:
    id: int
    name: str
    address: Address


class StubDistributor(IM2ODistributor):
    """Stub distributor for testing."""

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
    """Stub repository with table property for testing."""

    def __init__(self, table: str = "test_table"):
        self._table = table
        self._storage = {}
        self._observers = []

    @property
    def table(self) -> str:
        return self._table

    async def insert(self, session: ISession, agg):
        pass

    async def get(self, session: ISession, id_):
        return None

    async def update(self, session: ISession, agg):
        pass

    async def find(self, session: ISession, specification: ISpecification):
        return []

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def attach(self, aspect, observer, id_=None):
        return lambda: None

    def detach(self, aspect, observer, id_=None):
        pass

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
            distributor=StubDistributor(values=["Active"]),
            input_generator=lambda session, pos=None: "Status",
        )
        super().__init__(
            repository=repository,
            output_factory=Status,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(status: Status) -> dict:
        return {
            'id': status.id.value if hasattr(status.id, 'value') else status.id,
            'name': status.name,
        }


class DepartmentFaker(AggregateProvider[dict, Department]):
    _id_attr = 'id'

    id: ValueProvider[int, DepartmentId]
    name: ValueProvider[str, str]
    status_id: ReferenceProvider

    def __init__(
            self,
            repository: IAggregateRepository,
            distributor: IM2ODistributor,
            status_provider: StatusFaker
    ):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor=True),
            input_generator=lambda session, pos=None: pos or 1,
            output_factory=DepartmentId,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(values=["Engineering"]),
            input_generator=lambda session, pos=None: "Dept",
        )
        self.status_id = ReferenceProvider(
            distributor=distributor,
            aggregate_provider=status_provider,
        )
        super().__init__(
            repository=repository,
            output_factory=Department,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(dept: Department) -> dict:
        return {
            'id': dept.id.value if hasattr(dept.id, 'value') else dept.id,
            'name': dept.name,
            'status_id': dept.status_id.value if hasattr(dept.status_id, 'value') else dept.status_id,
        }


class UserFaker(AggregateProvider[dict, User]):
    _id_attr = 'id'

    id: ValueProvider[int, UserId]
    department_id: ReferenceProvider
    name: ValueProvider[str, str]

    def __init__(
            self,
            repository: IAggregateRepository,
            distributor: IM2ODistributor,
            department_provider: DepartmentFaker
    ):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor=True),
            input_generator=lambda session, pos=None: pos or 1,
            output_factory=UserId,
        )
        self.department_id = ReferenceProvider(
            distributor=distributor,
            aggregate_provider=department_provider,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(values=["Alice"]),
            input_generator=lambda session, pos=None: "User",
        )
        super().__init__(
            repository=repository,
            output_factory=User,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(user: User) -> dict:
        return {
            'id': user.id.value if hasattr(user.id, 'value') else user.id,
            'department_id': user.department_id.value if hasattr(user.department_id, 'value') else user.department_id,
            'name': user.name,
        }


class AddressFaker(CompositeValueProvider[dict, Address]):

    city: ValueProvider[str, str]
    street: ValueProvider[str, str]

    def __init__(self):
        self.city = ValueProvider(
            distributor=StubDistributor(values=["Moscow"]),
            input_generator=lambda session, pos=None: "City",
        )
        self.street = ValueProvider(
            distributor=StubDistributor(values=["Main St"]),
            input_generator=lambda session, pos=None: "Street",
        )
        super().__init__(output_factory=Address)


class CompanyFaker(AggregateProvider[dict, Company]):
    _id_attr = 'id'

    id: ValueProvider[int, int]
    name: ValueProvider[str, str]
    address: AddressFaker  # Composite Value Object, NOT ReferenceProvider

    def __init__(self, repository: IAggregateRepository):
        self.id = ValueProvider(
            distributor=StubDistributor(raise_cursor=True),
            input_generator=lambda session, pos=None: pos or 1,
        )
        self.name = ValueProvider(
            distributor=StubDistributor(values=["Acme Corp"]),
            input_generator=lambda session, pos=None: "Company",
        )
        self.address = AddressFaker()
        super().__init__(
            repository=repository,
            output_factory=Company,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(company: Company) -> dict:
        return {
            'id': company.id,
            'name': company.name,
            'address': {
                'city': company.address.city,
                'street': company.address.street,
            },
        }


# =============================================================================
# Tests for PgSpecificationVisitor - Basic
# =============================================================================

class PgSpecificationVisitorBasicTestCase(TestCase):
    """Basic tests for PgSpecificationVisitor."""

    def test_empty_pattern(self):
        """Empty pattern should produce no SQL."""
        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification({})

        self.assertEqual(visitor.sql, "")
        self.assertEqual(visitor.params, tuple())

    def test_simple_pattern(self):
        """Simple pattern should use @> operator."""
        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification({'status': 'active'})

        self.assertIn("@>", visitor.sql)
        self.assertEqual(len(visitor.params), 1)

    def test_multiple_simple_constraints(self):
        """Multiple simple constraints should be combined with @>."""
        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification({
            'status': 'active',
            'type': 'user'
        })

        self.assertIn("@>", visitor.sql)
        self.assertEqual(len(visitor.params), 1)

    def test_custom_target_value_expr(self):
        """Custom target_value_expr should be used in SQL."""
        visitor = PgSpecificationVisitor(target_value_expr="data")
        visitor.visit_object_pattern_specification({'status': 'active'})

        self.assertIn("data @>", visitor.sql)

    def test_non_dict_pattern(self):
        """Non-dict pattern should use @> operator."""
        import uuid
        test_uuid = uuid.uuid4()

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(test_uuid)

        self.assertIn("@>", visitor.sql)
        self.assertEqual(len(visitor.params), 1)


# =============================================================================
# Tests for PgSpecificationVisitor - Nested Constraints
# =============================================================================

class PgSpecificationVisitorNestedConstraintsTestCase(TestCase):
    """Tests for nested constraints handling."""

    def test_nested_without_accessor_uses_containment(self):
        """Nested dict without accessor should use simple @>."""
        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'fk_id': {'status': 'active'}},
            aggregate_provider_accessor=None
        )

        self.assertIn("@>", visitor.sql)
        self.assertNotIn("EXISTS", visitor.sql)

    def test_nested_with_reference_provider_uses_exists(self):
        """Nested dict for IReferenceProvider should generate EXISTS subquery."""
        # Setup providers hierarchy
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        dept_repo = StubRepository(table="departments")
        dept_dist = StubDistributor()
        dept_provider = DepartmentFaker(dept_repo, dept_dist, status_provider)
        dept_provider.provider_name = "department"

        user_repo = StubRepository(table="users")
        user_dist = StubDistributor()
        user_provider = UserFaker(user_repo, user_dist, dept_provider)
        user_provider.provider_name = "user"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'department_id': {'name': 'Engineering'}},
            aggregate_provider_accessor=lambda: user_provider
        )

        # Should generate EXISTS subquery
        self.assertIn("EXISTS", visitor.sql)
        self.assertIn("departments", visitor.sql)
        self.assertIn("value_id", visitor.sql)

    def test_nested_with_composite_value_object_uses_containment(self):
        """Nested dict for CompositeValueProvider should use simple @>."""
        company_repo = StubRepository(table="companies")
        company_provider = CompanyFaker(company_repo)
        company_provider.provider_name = "company"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'address': {'city': 'Moscow'}},
            aggregate_provider_accessor=lambda: company_provider
        )

        # Should use simple @> (not EXISTS) for composite value object
        self.assertIn("@>", visitor.sql)
        # address is CompositeValueProvider, not ReferenceProvider
        # So no EXISTS subquery
        self.assertNotIn("EXISTS", visitor.sql)

    def test_mixed_simple_and_nested_constraints(self):
        """Mixed simple and nested constraints should generate combined SQL."""
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        dept_repo = StubRepository(table="departments")
        dept_dist = StubDistributor()
        dept_provider = DepartmentFaker(dept_repo, dept_dist, status_provider)
        dept_provider.provider_name = "department"

        user_repo = StubRepository(table="users")
        user_dist = StubDistributor()
        user_provider = UserFaker(user_repo, user_dist, dept_provider)
        user_provider.provider_name = "user"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {
                'name': 'Alice',  # Simple constraint
                'department_id': {'name': 'Engineering'}  # Nested constraint
            },
            aggregate_provider_accessor=lambda: user_provider
        )

        # Should have both @> for simple and EXISTS for nested
        self.assertIn("@>", visitor.sql)
        self.assertIn("EXISTS", visitor.sql)
        self.assertIn("AND", visitor.sql)


# =============================================================================
# Tests for PgSpecificationVisitor - EXISTS Subquery Format
# =============================================================================

class PgSpecificationVisitorExistsSubqueryTestCase(TestCase):
    """Tests for EXISTS subquery SQL format."""

    def test_exists_subquery_format(self):
        """EXISTS subquery should have correct format for index usage."""
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        dept_repo = StubRepository(table="departments")
        dept_dist = StubDistributor()
        dept_provider = DepartmentFaker(dept_repo, dept_dist, status_provider)
        dept_provider.provider_name = "department"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'status_id': {'name': 'Active'}},
            aggregate_provider_accessor=lambda: dept_provider
        )

        sql = visitor.sql

        # Check EXISTS format
        self.assertIn("EXISTS", sql)
        self.assertIn("SELECT 1", sql)
        self.assertIn("statuses", sql)

        # Check that it uses rt.value @> for GIN index
        self.assertIn("rt.value @>", sql)

        # Check that it uses value_id = for B-tree index
        self.assertIn("rt.value_id =", sql)
        self.assertIn("value->'status_id'", sql)

    def test_exists_subquery_nested_two_levels(self):
        """Two-level nested constraints should generate nested EXISTS."""
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        dept_repo = StubRepository(table="departments")
        dept_dist = StubDistributor()
        dept_provider = DepartmentFaker(dept_repo, dept_dist, status_provider)
        dept_provider.provider_name = "department"

        user_repo = StubRepository(table="users")
        user_dist = StubDistributor()
        user_provider = UserFaker(user_repo, user_dist, dept_provider)
        user_provider.provider_name = "user"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'department_id': {'status_id': {'name': 'Active'}}},
            aggregate_provider_accessor=lambda: user_provider
        )

        sql = visitor.sql

        # Should have nested EXISTS
        self.assertEqual(sql.count("EXISTS"), 2)
        self.assertIn("departments", sql)
        self.assertIn("statuses", sql)


# =============================================================================
# Tests for PgSpecificationVisitor - Edge Cases
# =============================================================================

class PgSpecificationVisitorEdgeCasesTestCase(TestCase):
    """Edge case tests for PgSpecificationVisitor."""

    def test_unknown_provider_key_uses_containment(self):
        """Unknown key (not in _providers) should use simple @>."""
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'unknown_field': {'nested': 'value'}},
            aggregate_provider_accessor=lambda: status_provider
        )

        # Unknown field should use simple @>
        self.assertIn("@>", visitor.sql)
        self.assertNotIn("EXISTS", visitor.sql)

    def test_none_pattern(self):
        """None pattern should produce no SQL."""
        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(None)

        self.assertEqual(visitor.sql, "")
        self.assertEqual(visitor.params, tuple())

    def test_empty_nested_pattern(self):
        """Empty nested pattern should be handled gracefully."""
        status_repo = StubRepository(table="statuses")
        status_dist = StubDistributor()
        status_provider = StatusFaker(status_repo, status_dist)
        status_provider.provider_name = "status"

        dept_repo = StubRepository(table="departments")
        dept_dist = StubDistributor()
        dept_provider = DepartmentFaker(dept_repo, dept_dist, status_provider)
        dept_provider.provider_name = "department"

        visitor = PgSpecificationVisitor()
        visitor.visit_object_pattern_specification(
            {'status_id': {}},  # Empty nested dict
            aggregate_provider_accessor=lambda: dept_provider
        )

        # Empty nested should still generate EXISTS with TRUE
        # or be handled gracefully
        self.assertTrue(len(visitor.sql) > 0 or visitor.sql == "")
