import datetime
from dateutil import tz
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.domain.specification.scope_specification import ScopeSpecification
from ascetic_ddd.faker.infrastructure.tests.db import make_internal_pg_session_pool
from ascetic_ddd.faker.domain.distributors.m2o.factory import distributor_factory
from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession

# logging.basicConfig(level="DEBUG")


class SequenceDistributorTestCase(IsolatedAsyncioTestCase):
    distributor_factory = staticmethod(distributor_factory)

    @staticmethod
    async def value_factory(session: ISession, position: int | None = None):
        return datetime.datetime(2025, 4, 15, tzinfo=tz.tzutc()) + datetime.timedelta(hours=1) * position

    async def _make_session_pool(self):
        return await make_internal_pg_session_pool()

    async def asyncSetUp(self):
        self.null_weight = 0
        self.session_pool = await self._make_session_pool()
        self.dist = self.distributor_factory(null_weight=self.null_weight, sequence=True)
        self.dist.provider_name = 'path.Fk.fk_id'

    async def _next_with_factory(self, ts_session, specification=None):
        try:
            return await self.dist.next(ts_session, specification)
        except Cursor as cursor:
            value = await self.value_factory(ts_session, cursor.position)
            await cursor.append(ts_session, value)
            return value

    async def test_default_key(self):
        count = 10

        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = [await self._next_with_factory(ts_session) for _ in range(count)]

        self.assertListEqual(
            result,
            [datetime.datetime(2025, 4, 15, tzinfo=tz.tzutc()) + datetime.timedelta(hours=1) * i for i in range(count)]
        )

    async def test_specific_key(self):
        count = 10

        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = [await self._next_with_factory(ts_session, ScopeSpecification(2)) for _ in range(count)]

        self.assertListEqual(
            result,
            [datetime.datetime(2025, 4, 15, tzinfo=tz.tzutc()) + datetime.timedelta(hours=1) * i for i in range(count)]
        )

        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = [await self._next_with_factory(ts_session, ScopeSpecification(3)) for _ in range(count)]

        self.assertListEqual(
            result,
            [datetime.datetime(2025, 4, 15, tzinfo=tz.tzutc()) + datetime.timedelta(hours=1) * i for i in range(count)]
        )

    async def asyncTearDown(self):
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            await self.dist.cleanup(ts_session)
        await self.session_pool._pool.close()
