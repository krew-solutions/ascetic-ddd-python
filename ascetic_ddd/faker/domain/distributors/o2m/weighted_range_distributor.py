import random
import typing

from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor

__all__ = ('WeightedRangeDistributor',)


class WeightedRangeDistributor(IO2MDistributor):
    """
    O2M дистрибьютор для выбора целого числа из ограниченного диапазона.

    В отличие от WeightedDistributor (который возвращает количество вокруг mean),
    этот дистрибьютор возвращает значение строго из диапазона [min_val, max_val].

    Параметры:
    - min_val: минимальное значение (включительно)
    - max_val: максимальное значение (включительно)
    - weights: веса для каждого значения в диапазоне (опционально)

    Примеры:
        # Равномерное распределение [0, 5]
        dist = WeightedRangeDistributor(0, 5)
        value = dist.distribute()  # 0, 1, 2, 3, 4 или 5

        # Взвешенное: 0 чаще, 5 реже
        dist = WeightedRangeDistributor(0, 5, weights=[0.5, 0.25, 0.12, 0.07, 0.04, 0.02])
        value = dist.distribute()

        # Взвешенное с меньшим числом весов (интерполяция на весь диапазон)
        dist = WeightedRangeDistributor(0, 5, weights=[0.7, 0.2, 0.1])
        value = dist.distribute()  # 0-5, веса интерполированы
    """
    _min_val: int
    _max_val: int
    _weights: list[float]
    _cumulative: list[float]

    def __init__(
            self,
            min_val: int,
            max_val: int,
            weights: typing.Iterable[float] | None = None,
    ):
        if min_val > max_val:
            raise ValueError(f"min_val ({min_val}) must be <= max_val ({max_val})")

        self._min_val = min_val
        self._max_val = max_val
        range_size = max_val - min_val + 1

        if weights is not None:
            weights_list = list(weights)
            if len(weights_list) == range_size:
                self._weights = weights_list
            elif len(weights_list) > range_size:
                # Обрезаем если весов больше
                self._weights = weights_list[:range_size]
            else:
                # Интерполируем веса на весь диапазон
                self._weights = self._interpolate_weights(weights_list, range_size)
        else:
            # Равномерное распределение
            self._weights = [1.0] * range_size

        # Нормализуем и вычисляем cumulative для быстрого выбора
        total = sum(self._weights)
        if total <= 0:
            raise ValueError("Sum of weights must be > 0")

        self._cumulative = []
        cumsum = 0.0
        for w in self._weights:
            cumsum += w / total
            self._cumulative.append(cumsum)

    @staticmethod
    def _interpolate_weights(weights: list[float], target_size: int) -> list[float]:
        """
        Линейная интерполяция весов на целевой размер.

        Пример: weights=[0.7, 0.2, 0.1], target_size=6
        Результат: веса распределены равномерно по позициям 0-5
        """
        if len(weights) < 2:
            return weights * target_size if weights else [1.0] * target_size

        result = []
        src_len = len(weights)
        for i in range(target_size):
            # Позиция в исходном массиве (дробная)
            src_pos = i * (src_len - 1) / (target_size - 1)
            # Индексы соседних весов
            left_idx = int(src_pos)
            right_idx = min(left_idx + 1, src_len - 1)
            # Доля между соседями
            frac = src_pos - left_idx
            # Линейная интерполяция
            interpolated = weights[left_idx] * (1 - frac) + weights[right_idx] * frac
            result.append(interpolated)

        return result

    def distribute(self) -> int:
        """
        Возвращает случайное значение из диапазона [min_val, max_val].

        Returns:
            Целое число с учётом весов.
        """
        r = random.random()

        # Binary search для O(log n)
        left, right = 0, len(self._cumulative) - 1
        while left < right:
            mid = (left + right) // 2
            if self._cumulative[mid] < r:
                left = mid + 1
            else:
                right = mid

        return self._min_val + left

    @classmethod
    def uniform(cls, min_val: int, max_val: int) -> 'WeightedRangeDistributor':
        """Равномерное распределение."""
        return cls(min_val, max_val)

    @classmethod
    def linear_decay(cls, min_val: int, max_val: int) -> 'WeightedRangeDistributor':
        """Линейно убывающие веса: первые значения чаще."""
        range_size = max_val - min_val + 1
        weights = [range_size - i for i in range(range_size)]
        return cls(min_val, max_val, weights=weights)

    @classmethod
    def exponential_decay(cls, min_val: int, max_val: int, decay: float = 0.5) -> 'WeightedRangeDistributor':
        """
        Экспоненциально убывающие веса.

        Args:
            decay: коэффициент затухания (0 < decay < 1).
                   Меньше = быстрее затухание.
        """
        if not 0 < decay < 1:
            raise ValueError("decay must be between 0 and 1")

        range_size = max_val - min_val + 1
        weights = [decay ** i for i in range(range_size)]
        return cls(min_val, max_val, weights=weights)

    @classmethod
    def pareto_like(cls, min_val: int, max_val: int, alpha: float = 2.0) -> 'WeightedRangeDistributor':
        """
        Парето-подобное распределение: правило 80/20.

        Args:
            alpha: параметр формы (больше = равномернее)
        """
        range_size = max_val - min_val + 1
        # weight(i) ∝ (i+1)^(-alpha)
        weights = [(i + 1) ** (-alpha) for i in range(range_size)]
        return cls(min_val, max_val, weights=weights)
