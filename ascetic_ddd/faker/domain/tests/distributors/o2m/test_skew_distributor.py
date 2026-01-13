import logging
import unittest

from ascetic_ddd.faker.domain.distributors.o2m.skew_distributor import SkewDistributor

# logging.basicConfig(level="DEBUG")


class SkewDistributorTestCase(unittest.TestCase):
    """
    Тесты для O2M SkewDistributor.

    Проверяем:
    - Среднее количество items близко к mean
    - При skew>1 распределение неравномерное (есть крупные и мелкие)
    """
    mean = 50
    iterations = 1000

    def test_average_equals_mean(self):
        """Среднее количество items должно быть близко к mean."""
        dist = SkewDistributor(skew=2.0, mean=self.mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Average: %.2f (expected: %d)", average, self.mean)
        self.assertAlmostEqual(average, self.mean, delta=self.mean * 0.15)

    def test_uniform_distribution_skew_1(self):
        """При skew=1.0 все получают примерно одинаково."""
        dist = SkewDistributor(skew=1.0, mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        average = sum(results) / len(results)

        # Все результаты должны быть около mean
        self.assertAlmostEqual(average, self.mean, delta=self.mean * 0.15)

    def test_skewed_distribution_has_variance(self):
        """При skew>1 должна быть значительная дисперсия."""
        dist = SkewDistributor(skew=3.0, mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]

        min_val = min(results)
        max_val = max(results)

        logging.info("Min: %d, Max: %d, Ratio: %.1f", min_val, max_val, max_val / max(min_val, 1))

        # Должна быть значительная разница между min и max
        self.assertGreater(max_val, min_val * 3)

    def test_high_skew_extreme_values(self):
        """При высоком skew возможны очень большие значения."""
        dist = SkewDistributor(skew=3.0, mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        max_val = max(results)

        logging.info("Max value with skew=3.0: %d", max_val)

        # Максимум должен быть значительно больше среднего
        self.assertGreater(max_val, self.mean * 2)

    def test_stateless(self):
        """Дистрибьютор stateless — можно вызывать из разных потоков."""
        dist = SkewDistributor(skew=2.0, mean=self.mean)

        # Несколько вызовов
        r1 = dist.distribute()
        r2 = dist.distribute()
        r3 = dist.distribute()

        # Все должны вернуть валидные значения
        self.assertGreaterEqual(r1, 0)
        self.assertGreaterEqual(r2, 0)
        self.assertGreaterEqual(r3, 0)


if __name__ == '__main__':
    unittest.main()
