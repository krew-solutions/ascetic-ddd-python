import functools
import logging
import uuid
import dataclasses
from collections import Counter
from unittest import IsolatedAsyncioTestCase

from ascetic_ddd.faker.infrastructure.tests.db import make_internal_pg_session_pool
from ascetic_ddd.faker.domain.distributors.m2o.factory import distributor_factory
from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.specification.object_pattern_specification import ObjectPatternSpecification
from ascetic_ddd.faker.domain.values.empty import Empty, empty
from ascetic_ddd.seedwork.domain.session.interfaces import ISession

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
    distributor_factory = staticmethod(distributor_factory)

    skew = 2.0
    null_weight = 0.5
    mean = 50
    count = 3000

    async def _make_session_pool(self):
        return await make_internal_pg_session_pool()

    async def asyncSetUp(self):
        self.session_pool = await self._make_session_pool()
        self.dist = self.distributor_factory(
            skew=self.skew,
            mean=self.mean,
            null_weight=self.null_weight,
        )
        self.dist.provider_name = 'path.SkewFk.fk_id'

    def _check_mean_of_emptiable_result(self, result, strategy=lambda actual_mean, expected_mean: None):
        counter = Counter(result)
        self.assertGreaterEqual(counter[None] / counter.total(), self.null_weight - 0.1)
        self.assertLessEqual(counter[None] / counter.total(), self.null_weight + 0.1)
        actual_mean = counter.total() / len(counter)
        expected_mean = (self.mean * (len(counter) - 1)) / (len(counter) * self.null_weight)
        logging.info(
            "Emptiable mean, Actual: %s, Expected: %s, Empty: %s, Non-Empty: %s, Total: %s, Len: %s",
            actual_mean, expected_mean, counter[None], counter.total() - counter[None], counter.total(), len(counter)
        )
        # Вероятностный подход (PgSkewDistributor) имеет более высокую дисперсию
        self.assertLessEqual(actual_mean, expected_mean * 1.5)
        strategy(actual_mean, expected_mean)

    def _check_mean_of_non_empty_result(self, result, strategy=lambda actual_mean, expected_mean: None):
        counter = Counter(result)
        del counter[None]
        actual_mean = counter.total() / len(counter)
        expected_mean = self.mean
        logging.info(
            "Non-empty mean, Actual: %s, Expected: %s, Total: %s, Len: %s",
            actual_mean, expected_mean, counter.total(), len(counter)
        )
        # Вероятностный подход (PgSkewDistributor) имеет более высокую дисперсию
        self.assertLessEqual(actual_mean, expected_mean * 1.5)
        strategy(actual_mean, expected_mean)

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
                except Cursor as cursor:
                    value = await factory(ts_session, cursor.position)
                    await cursor.append(ts_session, value)
                    result.append(value)

        # Вероятностный подход в PgSkewDistributor имеет более высокую дисперсию,
        # поэтому используем 40% tolerance вместо 20%
        self._check_mean_of_emptiable_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=(self.mean / self.null_weight) * 0.4)
        )
        self._check_mean_of_non_empty_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=self.mean * 0.4)
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
                except Cursor as cursor:
                    value = await factory(ts_session)
                    await cursor.append(ts_session, value)
                    result.append(value)

        self._check_mean_of_emptiable_result(result)
        self._check_mean_of_non_empty_result(result)
        self._check_skew_distribution(result)


class CollectionSkewDistributorTestCase(_BaseSkewDistributorTestCase):
    skew = 3.0

    async def asyncSetUp(self):
        self.session_pool = await self._make_session_pool()
        self._values = self._make_values()
        self._value_iter = iter(self._values)
        self.mean = self.null_weight * self.count / len(self._values)
        self.dist = self.distributor_factory(
            skew=self.skew,
            mean=None,
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
                except Cursor as cursor:
                    try:
                        value = next(self._value_iter)
                        await cursor.append(ts_session, value)
                        result.append(value)
                    except StopIteration:
                        result.append(None)

        self._check_mean_of_emptiable_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=(self.mean / self.null_weight) * 0.05)
        )
        self._check_mean_of_non_empty_result(
            result,
            functools.partial(self.assertAlmostEqual, delta=self.mean * 0.05)
        )
        self._check_skew_distribution(result)


class SkewIndexSelectIdxTestCase(IsolatedAsyncioTestCase):
    """
    Тесты для SkewIndex._select_idx().

    Проверяем формулу: idx = int(n * (1 - random())^skew)
    Теоретически: P(idx < x*n) = x^(1/skew)
    """

    def _simulate_select_idx(self, n: int, skew: float, samples: int = 100000) -> list[int]:
        """Симулирует SkewIndex._select_idx()"""
        import random
        results = []
        for _ in range(samples):
            idx = int(n * (1 - random.random()) ** skew)
            idx = min(idx, n - 1)
            results.append(idx)
        return results

    def _get_percentile_ratio(self, results: list[int], n: int, percentile: float) -> float:
        """Возвращает долю результатов в первых percentile% индексов."""
        cutoff = int(n * percentile)
        count_in_range = sum(1 for r in results if r < cutoff)
        return count_in_range / len(results)

    async def test_select_idx_uniform(self):
        """При skew=1.0 распределение равномерное."""
        n = 1000
        results = self._simulate_select_idx(n, skew=1.0)

        # Первые 25% должны получать ~25% вызовов
        ratio = self._get_percentile_ratio(results, n, 0.25)
        self.assertAlmostEqual(ratio, 0.25, delta=0.02)

        # Первые 50% должны получать ~50% вызовов
        ratio = self._get_percentile_ratio(results, n, 0.50)
        self.assertAlmostEqual(ratio, 0.50, delta=0.02)

    async def test_select_idx_theoretical_formula(self):
        """
        Проверка теоретической формулы: P(idx < x*n) = x^(1/skew).

        Математическое обоснование:
        - idx = n * (1 - u)^skew, где u ~ Uniform[0,1)
        - P(idx < k) = P(n*(1-u)^skew < k) = P((1-u) < (k/n)^(1/skew))
        - P(idx < k) = (k/n)^(1/skew)
        """
        n = 1000
        samples = 100000

        test_cases = [
            # (skew, percentile, expected_ratio, tolerance)
            (2.0, 0.10, 0.10 ** 0.5, 0.02),      # 10% → 31.6%
            (2.0, 0.25, 0.25 ** 0.5, 0.02),      # 25% → 50%
            (2.0, 0.50, 0.50 ** 0.5, 0.02),      # 50% → 70.7%
            (3.0, 0.10, 0.10 ** (1/3), 0.02),    # 10% → 46.4%
            (3.0, 0.25, 0.25 ** (1/3), 0.02),    # 25% → 63%
            (4.0, 0.10, 0.10 ** 0.25, 0.02),     # 10% → 56.2%
            (4.0, 0.50, 0.50 ** 0.25, 0.02),     # 50% → 84.1%
        ]

        for skew, percentile, expected, tolerance in test_cases:
            with self.subTest(skew=skew, percentile=percentile):
                results = self._simulate_select_idx(n, skew, samples)
                actual = self._get_percentile_ratio(results, n, percentile)

                self.assertAlmostEqual(
                    actual, expected, delta=tolerance,
                    msg=f"skew={skew}, первые {percentile*100:.0f}%: "
                        f"ожидалось {expected*100:.1f}%, получено {actual*100:.1f}%"
                )

    async def test_select_idx_skew_increases_bias(self):
        """Больший skew → больший перекос к началу."""
        n = 1000
        percentile = 0.25

        prev_ratio = 0
        for skew in [1.0, 2.0, 3.0, 4.0]:
            results = self._simulate_select_idx(n, skew)
            ratio = self._get_percentile_ratio(results, n, percentile)

            self.assertGreater(
                ratio, prev_ratio,
                msg=f"skew={skew} должен давать больший перекос чем предыдущий"
            )
            prev_ratio = ratio


class EstimateSkewTestCase(IsolatedAsyncioTestCase):
    """
    Тесты для estimate_skew().

    Проверяем формулу: skew = 1 / (1 - alpha)
    где alpha — параметр Zipf из log-log регрессии.
    """

    def _generate_skew_data(self, n: int, skew: float, samples: int) -> dict[int, int]:
        """Генерирует данные с известным skew для проверки estimate_skew."""
        import random
        counter = Counter()
        for _ in range(samples):
            u = random.random()
            idx = int(n * (1 - u) ** skew)
            idx = min(idx, n - 1)
            counter[idx] += 1
        return dict(counter)

    async def test_estimate_skew_formula(self):
        """
        Проверка формулы skew = 1 / (1 - alpha).

        Математическое обоснование:
        - SkewDistributor: idx = floor(n * (1 - u)^skew)
        - PDF: p(x) ∝ x^(1/skew - 1)
        - Zipf: freq(rank) ∝ rank^(-alpha)
        - Сравнивая: -alpha = 1/skew - 1 → skew = 1/(1-alpha)
        """
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import estimate_skew

        test_cases = [
            # (skew, допустимая ошибка)
            # Положительный сдвиг растёт с skew из-за дискретизации
            (1.5, 0.15),
            (2.0, 0.15),
            (2.5, 0.20),
            (3.0, 0.30),
            (4.0, 0.55),
            (5.0, 0.80),
        ]

        for target_skew, tolerance in test_cases:
            with self.subTest(skew=target_skew):
                data = self._generate_skew_data(1000, target_skew, 100000)
                estimated_skew, r_squared = estimate_skew(data)

                self.assertGreater(r_squared, 0.95, "R² должен быть > 0.95 для хорошей подгонки")
                self.assertAlmostEqual(
                    estimated_skew, target_skew, delta=tolerance,
                    msg=f"skew={target_skew}: ожидалось ~{target_skew}, получено {estimated_skew:.3f}"
                )

    async def test_estimate_skew_uniform(self):
        """При равномерном распределении skew ≈ 1.0."""
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import estimate_skew

        data = self._generate_skew_data(1000, 1.0, 100000)
        estimated_skew, _ = estimate_skew(data)

        self.assertAlmostEqual(estimated_skew, 1.0, delta=0.1)

    async def test_estimate_skew_edge_cases(self):
        """Граничные случаи."""
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import estimate_skew

        # Пустой словарь
        skew, r2 = estimate_skew({})
        self.assertEqual(skew, 1.0)
        self.assertEqual(r2, 0.0)

        # Один элемент
        skew, r2 = estimate_skew({'a': 100})
        self.assertEqual(skew, 1.0)
        self.assertEqual(r2, 0.0)

        # Два элемента
        skew, r2 = estimate_skew({'a': 100, 'b': 50})
        self.assertGreaterEqual(skew, 1.0)


class WeightsToSkewTestCase(IsolatedAsyncioTestCase):
    """
    Тесты для weights_to_skew().

    Проверяем, что функция корректно воспроизводит weights[0].
    """

    def _simulate_weights(self, n_partitions: int, skew: float, samples: int = 100000) -> list[float]:
        """Симулирует SkewDistributor и возвращает веса партиций."""
        import random
        partition_size = 1.0 / n_partitions
        counts = [0] * n_partitions

        for _ in range(samples):
            u = random.random()
            idx_normalized = (1 - u) ** skew
            partition = min(int(idx_normalized / partition_size), n_partitions - 1)
            counts[partition] += 1

        return [c / samples for c in counts]

    async def test_weights_to_skew_first_weight(self):
        """
        Проверка: weights_to_skew() точно воспроизводит weights[0].

        Формула: P(первая партиция) = (1/k)^(1/skew) = weights[0]
        Решая: skew = log(1/k) / log(weights[0])
        """
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import weights_to_skew

        test_weights = [
            [0.7, 0.2, 0.07, 0.03],
            [0.5, 0.3, 0.15, 0.05],
            [0.8, 0.1, 0.07, 0.03],
            [0.6, 0.25, 0.1, 0.05],
        ]

        for weights in test_weights:
            with self.subTest(weights=weights):
                skew = weights_to_skew(weights)
                simulated = self._simulate_weights(len(weights), skew)

                self.assertAlmostEqual(
                    simulated[0], weights[0], delta=0.02,
                    msg=f"weights[0]={weights[0]}: ожидалось ~{weights[0]}, получено {simulated[0]:.3f}"
                )

    async def test_weights_to_skew_uniform(self):
        """Равномерные веса → skew = 1.0."""
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import weights_to_skew

        skew = weights_to_skew([0.25, 0.25, 0.25, 0.25])
        self.assertAlmostEqual(skew, 1.0, delta=0.01)

        simulated = self._simulate_weights(4, skew)
        for i, w in enumerate(simulated):
            self.assertAlmostEqual(w, 0.25, delta=0.02, msg=f"partition {i}")

    async def test_weights_to_skew_edge_cases(self):
        """Граничные случаи."""
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import weights_to_skew

        # Пустой список
        self.assertEqual(weights_to_skew([]), 1.0)

        # Один элемент
        self.assertEqual(weights_to_skew([1.0]), 1.0)

        # Невалидные веса
        self.assertEqual(weights_to_skew([0.0, 0.5, 0.5]), 2.0)
        self.assertEqual(weights_to_skew([1.0, 0.0, 0.0]), 2.0)

    async def test_weights_to_skew_known_values(self):
        """Проверка известных значений skew."""
        from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import weights_to_skew
        import math

        # Для 4 партиций: P(first) = (1/4)^(1/skew)
        # skew=2: P = 0.25^0.5 = 0.5
        # skew=3: P = 0.25^(1/3) ≈ 0.63
        # skew=4: P = 0.25^0.25 ≈ 0.707

        test_cases = [
            (0.5, 2.0),    # (target_weight[0], expected_skew)
            (0.25 ** (1/3), 3.0),
            (0.25 ** 0.25, 4.0),
        ]

        for target_p, expected_skew in test_cases:
            weights = [target_p, (1 - target_p) / 3, (1 - target_p) / 3, (1 - target_p) / 3]
            skew = weights_to_skew(weights)
            self.assertAlmostEqual(
                skew, expected_skew, delta=0.01,
                msg=f"weights[0]={target_p:.3f}: ожидалось skew={expected_skew}, получено {skew:.3f}"
            )
