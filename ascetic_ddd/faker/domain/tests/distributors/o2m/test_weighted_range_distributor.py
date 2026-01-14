import logging
import unittest

from ascetic_ddd.faker.domain.distributors.o2m.weighted_range_distributor import WeightedRangeDistributor

# logging.basicConfig(level="DEBUG")


class WeightedRangeDistributorTestCase(unittest.TestCase):
    """
    Тесты для O2M WeightedRangeDistributor.

    Проверяем:
    - Значения строго в диапазоне [min_val, max_val]
    - Распределение соответствует весам
    - Factory methods работают корректно
    """
    iterations = 10000

    def test_values_in_range(self):
        """Все значения в диапазоне [min_val, max_val]."""
        dist = WeightedRangeDistributor(0, 5)

        for _ in range(self.iterations):
            value = dist.distribute()
            self.assertGreaterEqual(value, 0)
            self.assertLessEqual(value, 5)

    def test_uniform_distribution(self):
        """Равномерное распределение: все значения примерно одинаково часты."""
        dist = WeightedRangeDistributor(0, 5)
        counts = {i: 0 for i in range(6)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        expected = self.iterations / 6
        for i, count in counts.items():
            logging.info("Value %d: %d (expected: %.0f)", i, count, expected)
            self.assertAlmostEqual(count, expected, delta=expected * 0.2)

    def test_weighted_distribution(self):
        """Взвешенное распределение соответствует весам."""
        weights = [0.5, 0.3, 0.15, 0.05]
        dist = WeightedRangeDistributor(0, 3, weights=weights)
        counts = {i: 0 for i in range(4)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        for i, w in enumerate(weights):
            expected = self.iterations * w
            logging.info("Value %d: %d (expected: %.0f)", i, counts[i], expected)
            self.assertAlmostEqual(counts[i], expected, delta=expected * 0.2)

    def test_weights_shorter_than_range_interpolation(self):
        """Веса короче диапазона: интерполяция на весь диапазон."""
        dist = WeightedRangeDistributor(0, 5, weights=[0.7, 0.2, 0.1])
        counts = {i: 0 for i in range(6)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        logging.info("Interpolated weights counts: %s", counts)

        # Все значения должны выбираться
        for i in range(6):
            self.assertGreater(counts[i], 0, f"Value {i} should be selected")

        # Убывающее распределение: 0 чаще всех, 5 реже всех
        self.assertGreater(counts[0], counts[5])

        # Монотонное убывание
        for i in range(5):
            self.assertGreaterEqual(
                counts[i], counts[i + 1] * 0.8,  # С допуском на статистику
                f"Value {i} should be >= {i+1}"
            )

    def test_interpolate_weights_method(self):
        """Проверка метода интерполяции напрямую."""
        # 3 веса -> 6 позиций
        weights = [0.7, 0.2, 0.1]
        result = WeightedRangeDistributor._interpolate_weights(weights, 6)

        self.assertEqual(len(result), 6)
        # Первый и последний должны соответствовать исходным
        self.assertAlmostEqual(result[0], 0.7)
        self.assertAlmostEqual(result[5], 0.1)
        # Монотонное убывание
        for i in range(5):
            self.assertGreaterEqual(result[i], result[i + 1])

    def test_interpolate_single_weight(self):
        """Интерполяция одного веса: все позиции одинаковы."""
        dist = WeightedRangeDistributor(0, 5, weights=[1.0])
        counts = {i: 0 for i in range(6)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        # Равномерное распределение
        expected = self.iterations / 6
        for i, count in counts.items():
            self.assertAlmostEqual(count, expected, delta=expected * 0.2)

    def test_single_value_range(self):
        """Диапазон из одного значения."""
        dist = WeightedRangeDistributor(5, 5)

        for _ in range(100):
            self.assertEqual(dist.distribute(), 5)

    def test_negative_range(self):
        """Отрицательные значения в диапазоне."""
        dist = WeightedRangeDistributor(-3, 3)

        values = set()
        for _ in range(self.iterations):
            value = dist.distribute()
            self.assertGreaterEqual(value, -3)
            self.assertLessEqual(value, 3)
            values.add(value)

        # Все значения должны встретиться
        self.assertEqual(values, {-3, -2, -1, 0, 1, 2, 3})

    def test_linear_decay(self):
        """Linear decay: первые значения чаще."""
        dist = WeightedRangeDistributor.linear_decay(0, 4)
        counts = {i: 0 for i in range(5)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        logging.info("Linear decay counts: %s", counts)

        # Каждое следующее значение должно быть реже
        for i in range(4):
            self.assertGreater(counts[i], counts[i + 1])

    def test_exponential_decay(self):
        """Exponential decay: первые значения значительно чаще."""
        dist = WeightedRangeDistributor.exponential_decay(0, 4, decay=0.5)
        counts = {i: 0 for i in range(5)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        logging.info("Exponential decay counts: %s", counts)

        # 0 должен быть значительно чаще чем 4
        self.assertGreater(counts[0], counts[4] * 5)

    def test_pareto_like(self):
        """Pareto-like: сильный перекос к первым значениям."""
        dist = WeightedRangeDistributor.pareto_like(0, 4, alpha=2.0)
        counts = {i: 0 for i in range(5)}

        for _ in range(self.iterations):
            counts[dist.distribute()] += 1

        logging.info("Pareto-like counts: %s", counts)

        # 0 должен быть значительно чаще
        self.assertGreater(counts[0], counts[1])
        self.assertGreater(counts[1], counts[2])

    def test_validation_min_greater_than_max(self):
        """Ошибка если min_val > max_val."""
        with self.assertRaises(ValueError):
            WeightedRangeDistributor(5, 0)

    def test_validation_zero_weights(self):
        """Ошибка если сумма весов = 0."""
        with self.assertRaises(ValueError):
            WeightedRangeDistributor(0, 5, weights=[0, 0, 0])

    def test_validation_decay_out_of_range(self):
        """Ошибка если decay вне (0, 1)."""
        with self.assertRaises(ValueError):
            WeightedRangeDistributor.exponential_decay(0, 5, decay=0)
        with self.assertRaises(ValueError):
            WeightedRangeDistributor.exponential_decay(0, 5, decay=1)
        with self.assertRaises(ValueError):
            WeightedRangeDistributor.exponential_decay(0, 5, decay=1.5)


if __name__ == '__main__':
    unittest.main()
