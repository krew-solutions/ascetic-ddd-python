import random

from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor

__all__ = ('SkewDistributor',)


class SkewDistributor(IO2MDistributor):
    """
    O2M дистрибьютор со степенным распределением.

    Параметры:
    - skew: степень перекоса (1.0 = равномерно, 2.0+ = перекос)
    - mean: среднее количество items на owner

    skew = 1.0 — все owners получают примерно одинаково (mean)
    skew = 2.0 — некоторые получают больше, большинство меньше
    skew = 3.0 — сильный перекос

    Пример:
        dist = SkewDistributor(skew=2.0, mean=50)
        devices_count = dist.distribute()  # среднее = 50
    """
    _skew: float
    _mean: float

    def __init__(
            self,
            skew: float = 2.0,
            mean: float | None = None,
    ):
        self._skew = max(1.0, skew)
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
        # При skew близком к 1.0 — равномерное распределение
        if self._skew <= 1.01:
            return self._mean

        # Степенное распределение: первые позиции получают больше
        # weight(pos) ∝ (1 - pos)^skew
        # Нормализуем так, чтобы среднее = mean

        # Вес для текущей позиции
        individual_weight = (1 - position) ** self._skew

        # Средний вес (интеграл от (1-x)^skew по x от 0 до 1)
        average_weight = 1.0 / (self._skew + 1)

        # Нормализованный expected
        return self._mean * individual_weight / average_weight

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
