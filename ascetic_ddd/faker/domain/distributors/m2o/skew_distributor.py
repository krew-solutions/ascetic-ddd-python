import math
import random
import typing

from ascetic_ddd.faker.domain.distributors.m2o import IM2ODistributor
from ascetic_ddd.faker.domain.distributors.m2o.weighted_distributor import BaseIndex, BaseDistributor
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = ('SkewDistributor', 'SkewIndex', 'estimate_skew', 'weights_to_skew')


def estimate_skew(usage_counts: dict[typing.Any, int], tail_cutoff: float = 0.9) -> tuple[float, float]:
    """
    Оценка параметра skew из реальных данных использования.

    Args:
        usage_counts: {value: count} — сколько раз каждое значение использовалось
        tail_cutoff: доля данных для анализа (отбросить хвост)

    Returns:
        (skew, r_squared) — параметр и качество подгонки (0-1)

    Пример:
        >>> counts = {'a': 100, 'b': 50, 'c': 25, 'd': 12}
        >>> skew, r2 = estimate_skew(counts)
        >>> dist = SkewDistributor(skew=skew)
    """
    if len(usage_counts) < 2:
        return 1.0, 0.0

    # Ранжируем по частоте (DESC)
    sorted_counts = sorted(usage_counts.values(), reverse=True)

    # Log-log данные (пропускаем нули и хвост)
    cutoff_idx = int(len(sorted_counts) * tail_cutoff)
    log_rank = []
    log_freq = []
    for rank, freq in enumerate(sorted_counts[:cutoff_idx], start=1):
        if freq > 0:
            log_rank.append(math.log(rank))
            log_freq.append(math.log(freq))

    if len(log_rank) < 2:
        return 1.0, 0.0

    # Линейная регрессия: log_freq = -alpha * log_rank + const
    n = len(log_rank)
    sum_x = sum(log_rank)
    sum_y = sum(log_freq)
    sum_xy = sum(x * y for x, y in zip(log_rank, log_freq))
    sum_x2 = sum(x * x for x in log_rank)
    sum_y2 = sum(y * y for y in log_freq)

    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 1.0, 0.0

    # Наклон (alpha) — отрицательный для убывающего распределения
    alpha = -(n * sum_xy - sum_x * sum_y) / denom

    # R² — качество подгонки
    ss_tot = sum_y2 - sum_y ** 2 / n
    if ss_tot == 0:
        r_squared = 0.0
    else:
        mean_y = sum_y / n
        ss_res = sum((y - (mean_y - alpha * (x - sum_x / n))) ** 2
                     for x, y in zip(log_rank, log_freq))
        r_squared = max(0, 1 - ss_res / ss_tot)

    # skew из alpha: skew = 1 / (1 - alpha)
    # Вывод: p(x) ∝ x^(1/skew - 1), Zipf: freq ∝ rank^(-alpha)
    # Сравнивая показатели: -alpha = 1/skew - 1 → skew = 1/(1-alpha)
    alpha = max(0, min(alpha, 0.9))  # ограничить: alpha >= 0.9 → skew >= 10
    skew = 1.0 / (1.0 - alpha) if alpha < 1.0 else 10.0

    return skew, r_squared


def weights_to_skew(weights: list[float]) -> float:
    """
    Конвертация списка весов в параметр skew.

    Для степенного распределения idx = n * (1-r)^skew:
    P(первый квартиль) = (1/len(weights))^(1/skew)

    Подбираем skew чтобы первый квартиль ≈ weights[0].

    Args:
        weights: список весов партиций (например [0.7, 0.2, 0.07, 0.03])

    Returns:
        skew: параметр для SkewDistributor

    Пример:
        >>> skew = weights_to_skew([0.7, 0.2, 0.07, 0.03])
        >>> skew  # ≈ 3.89
    """
    if not weights or len(weights) < 2:
        return 1.0

    target_q1 = weights[0]
    q = 1 / len(weights)

    if target_q1 <= 0 or target_q1 >= 1:
        return 2.0

    skew = math.log(q) / math.log(target_q1)
    return max(1.0, min(skew, 10.0))


T = typing.TypeVar("T", covariant=True)


# =============================================================================
# SkewIndex
# =============================================================================

class SkewIndex(BaseIndex[T], typing.Generic[T]):
    """
    Индекс со степенным распределением.
    Один параметр skew вместо списка весов.

    skew = 1.0 — равномерное распределение
    skew = 2.0 — умеренный перекос к началу (первые значения чаще)
    skew = 3.0+ — сильный перекос
    """
    _skew: float

    def __init__(self, skew: float, specification: ISpecification[T]):
        self._skew = skew
        super().__init__(specification)

    def _select_idx(self) -> int:
        """Выбирает индекс со степенным распределением. O(1)"""
        n = len(self._values)
        # Степенное распределение: idx = n * (1 - random)^skew
        # При skew=1: равномерное (25% в каждом квартиле)
        # При skew=2: перекос к началу (50% в первом квартиле)
        # При skew=3: сильный перекос (63% в первом квартиле)
        idx = int(n * (1 - random.random()) ** self._skew)
        return min(idx, n - 1)


# =============================================================================
# SkewDistributor
# =============================================================================

class SkewDistributor(BaseDistributor[T], typing.Generic[T]):
    """
    Дистрибьютор со степенным распределением.

    Один параметр skew вместо списка весов:
    - skew = 1.0 — равномерное распределение
    - skew = 2.0 — умеренный перекос (первые 20% получают ~60% вызовов)
    - skew = 3.0 — сильный перекос (первые 10% получают ~70% вызовов)

    Преимущества:
    - O(1) выбор значения (vs O(n) у Distributor)
    - Один параметр вместо списка весов
    - Нет проблемы миграции значений между индексами
    """
    _skew: float

    def __init__(
            self,
            delegate: IM2ODistributor[T],
            skew: float = 2.0,
            mean: float | None = None,
    ):
        self._skew = skew
        super().__init__(delegate=delegate, mean=mean)

    def _create_index(self, specification: ISpecification[T]) -> SkewIndex[T]:
        return SkewIndex(self._skew, specification)
