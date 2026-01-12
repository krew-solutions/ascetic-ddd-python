import functools
import logging
import uuid
import dataclasses
from collections import Counter
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.infrastructure.tests.db import make_internal_pg_session_pool
from ascetic_ddd.faker.domain.distributors import skew_distributor_factory
from ascetic_ddd.faker.domain.specification.object_pattern_specification import ObjectPatternSpecification
from ascetic_ddd.faker.domain.values.empty import Empty, empty
from ascetic_ddd.faker.domain.session.interfaces import ISession

# logging.basicConfig(level="DEBUG")


@dataclasses.dataclass(kw_only=True)
class SomePk:
    id: uuid.UUID | Empty
    another_model_id: uuid.UUID

    def __hash__(self):
        assert self.id is not empty
        assert self.another_model_id is not empty
        return hash((self.id, self.another_model_id))


class Factory:
    another_model_id: uuid.UUID

    async def __call__(self, _session: ISession):
        return SomePk(
            id=uuid.uuid4(),
            another_model_id=self.another_model_id
        )


class _BaseSkewDistributorTestCase(IsolatedAsyncioTestCase):
    """
    Базовый класс тестов для SkewDistributor.

    Проверяем степенное распределение: idx = n * (1 - random())^skew
    При skew=1: равномерное распределение
    При skew=2: первые 50% значений получают ~75% вызовов
    При skew=3: первые 33% значений получают ~70% вызовов
    """
    distributor_factory = staticmethod(skew_distributor_factory)

    skew = 2.0
    null_weight = 0.5
    scale = 50
    count = 3000

    async def _make_session_pool(self):
        return await make_internal_pg_session_pool()

    async def asyncSetUp(self):
        self.session_pool = await self._make_session_pool()
        self.dist = self.distributor_factory(
            skew=self.skew,
            scale=self.scale,
            null_weight=self.null_weight,
        )
        self.dist.provider_name = 'path.SkewFk.fk_id'

    def _check_scale_of_emptiable_result(self, result, strategy=lambda actual_scale, expected_scale: None):
        counter = Counter(result)
        self.assertGreaterEqual(counter[None] / counter.total(), self.null_weight - 0.1)
        self.assertLessEqual(counter[None] / counter.total(), self.null_weight + 0.1)
        actual_scale = counter.total() / len(counter)
        expected_scale = (self.scale * (len(counter) - 1)) / (len(counter) * self.null_weight)
        logging.info(
            "Emptiable scale, Actual: %s, Expected: %s, Empty: %s, Non-Empty: %s, Total: %s, Len: %s",
            actual_scale, expected_scale, counter[None], counter.total() - counter[None], counter.total(), len(counter)
        )
        # Вероятностный подход (PgSkewDistributor) имеет более высокую дисперсию
        self.assertLessEqual(actual_scale, expected_scale * 1.5)
        strategy(actual_scale, expected_scale)

    def _check_scale_of_non_empty_result(self, result, strategy=lambda actual_scale, expected_scale: None):
        counter = Counter(result)
        del counter[None]
        actual_scale = counter.total() / len(counter)
        expected_scale = self.scale
        logging.info(
            "Non-empty scale, Actual: %s, Expected: %s, Total: %s, Len: %s",
            actual_scale, expected_scale, counter.total(), len(counter)
        )
        # Вероятностный подход (PgSkewDistributor) имеет более высокую дисперсию
        self.assertLessEqual(actual_scale, expected_scale * 1.5)
        strategy(actual_scale, expected_scale)

    def _check_skew_distribution(self, result):
        """Проверка перекоса: первая половина должна получать больше вызовов."""
        counter = Counter(result)
        if None in counter:
            del counter[None]

        counts = list(sorted(counter.values(), reverse=True))
        n = len(counts)
        first_half_sum = sum(counts[:n // 2])
        second_half_sum = sum(counts[n // 2:])
        first_half_ratio = first_half_sum / counter.total()

        logging.info(
            "First half: %d (%.1f%%), Second half: %d (%.1f%%)",
            first_half_sum, first_half_ratio * 100,
            second_half_sum, (1 - first_half_ratio) * 100
        )

        # При skew>=2 первая половина должна получать значительно больше
        self.assertGreater(first_half_ratio, 0.6)

    async def asyncTearDown(self):
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            await self.dist.cleanup(ts_session)
        await self.session_pool._pool.close()


class DefaultKeySkewDistributorTestCase(_BaseSkewDistributorTestCase):

    async def test_default_key(self):
        val = 0

        async def factory(_session: ISession, _position: int | None = None):
            nonlocal val
            res = val
            val += 1
            return res

        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = []
            for _ in range(self.count):
                try:
                    result.append(await self.dist.next(ts_session))
                except StopAsyncIteration as e:
                    value = await factory(ts_session, e.args[0] if e.args else None)
                    await self.dist.append(ts_session, value)
                    result.append(value)

        # Вероятностный подход в PgSkewDistributor имеет более высокую дисперсию,
        # поэтому используем 40% tolerance вместо 20%
        self._check_scale_of_emptiable_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=(self.scale / self.null_weight) * 0.4)
        )
        self._check_scale_of_non_empty_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=self.scale * 0.4)
        )
        self._check_skew_distribution(result)


class SpecificKeySkewDistributorTestCase(_BaseSkewDistributorTestCase):

    async def test_specific_key(self):
        factory = Factory()

        factory.another_model_id = uuid.uuid4()
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = []
            for i in range(self.count):
                if i % 200 == 0:
                    factory.another_model_id = uuid.uuid4()
                spec = ObjectPatternSpecification(
                    dict(another_model_id=factory.another_model_id),
                    lambda obj: dataclasses.asdict(obj)
                )
                try:
                    result.append(await self.dist.next(ts_session, specification=spec))
                except StopAsyncIteration:
                    value = await factory(ts_session)
                    await self.dist.append(ts_session, value)
                    result.append(value)

        self._check_scale_of_emptiable_result(result)
        self._check_scale_of_non_empty_result(result)
        self._check_skew_distribution(result)


class CollectionSkewDistributorTestCase(_BaseSkewDistributorTestCase):
    skew = 3.0

    async def asyncSetUp(self):
        self.session_pool = await self._make_session_pool()
        self._values = self._make_values()
        self._value_iter = iter(self._values)
        self.scale = self.null_weight * self.count / len(self._values)
        self.dist = self.distributor_factory(
            skew=self.skew,
            scale=None,
            null_weight=self.null_weight,
        )
        self.dist.provider_name = 'path.SkewFk.fk_id'

    def _make_values(self):
        return [5, 10, 20]

    async def test_fixed_collection(self):

        async with self.session_pool.session() as session, session.atomic() as ts_session:
            result = []
            for _ in range(self.count):
                try:
                    result.append(await self.dist.next(ts_session))
                except StopAsyncIteration:
                    try:
                        value = next(self._value_iter)
                        await self.dist.append(ts_session, value)
                        result.append(value)
                    except StopIteration:
                        result.append(None)

        self._check_scale_of_emptiable_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=(self.scale / self.null_weight) * 0.05)
        )
        self._check_scale_of_non_empty_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=self.scale * 0.05)
        )
        self._check_skew_distribution(result)
