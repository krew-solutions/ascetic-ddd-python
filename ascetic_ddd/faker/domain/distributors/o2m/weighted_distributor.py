import math
import random
import typing

from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor

__all__ = ('WeightedDistributor',)


class WeightedDistributor(IO2MDistributor):
    """
    O2M дистрибьютор с взвешенным распределением по партициям.

    Параметры:
    - weights: веса партиций (например [0.7, 0.2, 0.07, 0.03])
    - mean: среднее количество items на owner

    Пример: weights=[0.7, 0.2, 0.07, 0.03], mean=50
    - 25% вызовов попадут в партицию 0 (крупные) — получат больше mean
    - 25% вызовов попадут в партицию 3 (мелкие) — получат меньше mean
    - Среднее по всем вызовам = mean

    Пример:
        dist = WeightedDistributor(weights=[0.7, 0.2, 0.07, 0.03], mean=50)
        devices_count = dist.distribute()  # среднее = 50
    """
    _weights: list[float]
    _mean: float

    def __init__(
            self,
            weights: typing.Iterable[float] = tuple(),
            mean: float | None = None,
    ):
        self._weights = list(weights) if weights else [0.7, 0.2, 0.07, 0.03]
        if not self._weights:
            self._weights = [1.0]
        self._mean = mean if mean is not None else 50.0

    def distribute(self) -> int:
        """
        Возвращает количество items.

        Returns:
            Случайное количество items (из распределения Пуассона).
            Среднее по всем вызовам = mean.
        """
        # Выбираем случайную позицию в распределении [0, 1)
        position = random.random()

        # Вычисляем ожидаемое количество для этой позиции
        expected = self._compute_expected_for_position(position)

        if expected <= 0:
            return 0

        return self._poisson(expected)

    def _compute_expected_for_position(self, position: float) -> float:
        """Вычисляет ожидаемое количество items для позиции в распределении."""
        num_partitions = len(self._weights)

        # Нормализуем веса
        total_weight = sum(self._weights)
        if total_weight == 0:
            return self._mean

        # Размер партиции (доля от позиций)
        partition_size = 1.0 / num_partitions

        # Определяем партицию по позиции
        partition_idx = min(int(position / partition_size), num_partitions - 1)

        # Позиция внутри партиции [0, 1)
        local_position = (position - partition_idx * partition_size) / partition_size

        # Вес партиции (доля items)
        partition_weight = self._weights[partition_idx] / total_weight

        # Среднее для этой партиции = mean * partition_weight * num_partitions
        # (т.к. партиция содержит 1/num_partitions позиций, но получает partition_weight items)
        average_in_partition = self._mean * partition_weight * num_partitions

        # Local skew внутри партиции (как в M2O)
        if partition_idx > 0 and self._weights[partition_idx] > 0:
            ratio = self._weights[partition_idx - 1] / self._weights[partition_idx]
            local_skew = max(1.0, math.log2(ratio) + 1)
        else:
            local_skew = 1.0

        # При local_skew=1: равномерное распределение внутри партиции
        if local_skew <= 1.01:
            return average_in_partition

        # Степенное распределение внутри партиции
        individual_weight = (1 - local_position) ** local_skew
        average_weight = 1.0 / (local_skew + 1)

        return average_in_partition * individual_weight / average_weight

    @staticmethod
    def _poisson(lam: float) -> int:
        """Генерирует случайное число из распределения Пуассона."""
        if lam > 30:
            result = random.gauss(lam, lam ** 0.5)
            return max(0, round(result))

        L = 2.718281828 ** (-lam)
        k = 0
        p = 1.0
        while p > L:
            k += 1
            p *= random.random()
        return k - 1
