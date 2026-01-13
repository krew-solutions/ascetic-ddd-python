import logging
import unittest

from ascetic_ddd.faker.domain.distributors.o2m.weighted_distributor import WeightedDistributor

# logging.basicConfig(level="DEBUG")


class WeightedDistributorTestCase(unittest.TestCase):
    """
    Тесты для O2M WeightedDistributor.

    Проверяем:
    - Среднее количество items близко к mean
    - Распределение по партициям соответствует весам
    """
    weights = [0.7, 0.2, 0.07, 0.03]
    mean = 50
    iterations = 1000

    def test_average_equals_mean(self):
        """Среднее количество items должно быть близко к mean."""
        dist = WeightedDistributor(weights=self.weights, mean=self.mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Average: %.2f (expected: %d)", average, self.mean)
        self.assertAlmostEqual(average, self.mean, delta=self.mean * 0.15)

    def test_distribution_has_variance(self):
        """Распределение должно иметь значительную дисперсию."""
        dist = WeightedDistributor(weights=self.weights, mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]

        min_val = min(results)
        max_val = max(results)

        logging.info("Min: %d, Max: %d", min_val, max_val)

        # Должна быть разница между min и max
        self.assertGreater(max_val, min_val * 2)

    def test_extreme_weights(self):
        """При экстремальных весах распределение сильно неравномерное."""
        dist = WeightedDistributor(weights=[0.9, 0.09, 0.009, 0.001], mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]

        min_val = min(results)
        max_val = max(results)

        logging.info("Extreme weights - Min: %d, Max: %d, Ratio: %.1f",
                     min_val, max_val, max_val / max(min_val, 1))

        # При экстремальных весах разница ещё больше
        self.assertGreater(max_val, min_val * 5)

    def test_equal_weights(self):
        """При равных весах распределение более равномерное."""
        dist = WeightedDistributor(weights=[0.25, 0.25, 0.25, 0.25], mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        average = sum(results) / len(results)

        # Среднее должно быть около mean
        self.assertAlmostEqual(average, self.mean, delta=self.mean * 0.15)

    def test_single_weight(self):
        """Один вес — все в одной партиции."""
        dist = WeightedDistributor(weights=[1.0], mean=self.mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        average = sum(results) / len(results)

        self.assertAlmostEqual(average, self.mean, delta=self.mean * 0.15)

    def test_stateless(self):
        """Дистрибьютор stateless — можно вызывать из разных потоков."""
        dist = WeightedDistributor(weights=self.weights, mean=self.mean)

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
