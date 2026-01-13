import logging
import random
import unittest

from ascetic_ddd.faker.domain.distributors.o2m.distribution_distributor import DistributionDistributor

# logging.basicConfig(level="DEBUG")


class DistributionDistributorTestCase(unittest.TestCase):
    """
    Тесты для O2M DistributionDistributor.

    Проверяем:
    - Среднее близко к target_mean для разных распределений
    - Разные распределения дают разную форму
    - Factory methods работают корректно
    """
    target_mean = 50
    iterations = 1000

    def test_exponential_average(self):
        """Экспоненциальное распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.exponential(target_mean=self.target_mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Exponential average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_exponential_has_long_tail(self):
        """Экспоненциальное распределение: есть длинный хвост."""
        dist = DistributionDistributor.exponential(target_mean=self.target_mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        max_val = max(results)
        median = sorted(results)[len(results) // 2]

        logging.info("Exponential - Max: %d, Median: %d", max_val, median)

        # Максимум должен быть значительно больше медианы
        self.assertGreater(max_val, median * 2)

    def test_pareto_average(self):
        """Парето распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.pareto(alpha=2.5, target_mean=self.target_mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Pareto average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.2)

    def test_pareto_extreme_values(self):
        """Парето распределение: есть экстремальные значения."""
        dist = DistributionDistributor.pareto(alpha=2.0, target_mean=self.target_mean)

        results = [dist.distribute() for _ in range(self.iterations)]
        max_val = max(results)

        logging.info("Pareto max: %d (target_mean: %d)", max_val, self.target_mean)

        # Парето может давать очень большие значения
        self.assertGreater(max_val, self.target_mean * 3)

    def test_lognormal_average(self):
        """Логнормальное распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.lognormal(sigma=0.5, target_mean=self.target_mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Lognormal average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_gamma_average(self):
        """Гамма распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.gamma(shape=2.0, target_mean=self.target_mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Gamma average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_weibull_average(self):
        """Вейбулла распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.weibull(shape=1.5, target_mean=self.target_mean)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Weibull average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_uniform_average(self):
        """Равномерное распределение: среднее близко к target_mean."""
        dist = DistributionDistributor.uniform(target_mean=self.target_mean, spread=0.5)

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Uniform average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.1)

    def test_uniform_bounded(self):
        """Равномерное распределение: значения в пределах spread."""
        dist = DistributionDistributor.uniform(target_mean=self.target_mean, spread=0.3)

        results = [dist.distribute() for _ in range(self.iterations)]
        min_val = min(results)
        max_val = max(results)

        logging.info("Uniform - Min: %d, Max: %d", min_val, max_val)

        # Должны быть в пределах [target_mean*0.7, target_mean*1.3] примерно
        self.assertGreaterEqual(min_val, self.target_mean * 0.5)
        self.assertLessEqual(max_val, self.target_mean * 1.7)

    def test_custom_sampler(self):
        """Кастомный sampler работает."""
        dist = DistributionDistributor(
            sampler=lambda: random.expovariate(1),
            sampler_mean=1.0,
            target_mean=self.target_mean,
        )

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_scipy_distribution(self):
        """scipy.stats distribution работает (если scipy доступен)."""
        try:
            from scipy import stats
        except ImportError:
            self.skipTest("scipy not installed")

        dist = DistributionDistributor(
            distribution=stats.expon(),
            target_mean=self.target_mean,
        )

        total = sum(dist.distribute() for _ in range(self.iterations))
        average = total / self.iterations

        logging.info("Scipy expon average: %.2f (expected: %d)", average, self.target_mean)
        self.assertAlmostEqual(average, self.target_mean, delta=self.target_mean * 0.15)

    def test_validation_no_distribution_or_sampler(self):
        """Ошибка если не указан ни distribution, ни sampler."""
        with self.assertRaises(ValueError):
            DistributionDistributor(target_mean=self.target_mean)

    def test_validation_both_distribution_and_sampler(self):
        """Ошибка если указаны оба: distribution и sampler."""
        try:
            from scipy import stats
            with self.assertRaises(ValueError):
                DistributionDistributor(
                    distribution=stats.expon(),
                    sampler=lambda: 1.0,
                    target_mean=self.target_mean,
                )
        except ImportError:
            self.skipTest("scipy not installed")

    def test_validation_sampler_without_sampler_mean(self):
        """Ошибка если sampler без sampler_mean."""
        with self.assertRaises(ValueError):
            DistributionDistributor(
                sampler=lambda: 1.0,
                target_mean=self.target_mean,
            )

    def test_pareto_invalid_alpha(self):
        """Парето с alpha <= 1 вызывает ошибку."""
        with self.assertRaises(ValueError):
            DistributionDistributor.pareto(alpha=1.0, target_mean=self.target_mean)

    def test_stateless(self):
        """Дистрибьютор stateless."""
        dist = DistributionDistributor.exponential(target_mean=self.target_mean)

        r1 = dist.distribute()
        r2 = dist.distribute()
        r3 = dist.distribute()

        self.assertGreaterEqual(r1, 0)
        self.assertGreaterEqual(r2, 0)
        self.assertGreaterEqual(r3, 0)


if __name__ == '__main__':
    unittest.main()
