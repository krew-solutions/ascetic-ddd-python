import math
import random
import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.observable.observable import Observable

__all__ = ('SkewDistributor', 'estimate_skew', 'weights_to_skew')


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

    # skew из alpha: при alpha≈1 (Zipf) skew≈2
    # Эмпирическая формула: skew ≈ 1 + alpha
    skew = 1.0 + max(0, alpha)
    skew = max(1.0, min(skew, 10.0))  # ограничить разумным диапазоном

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


class SkewPartition(typing.Generic[T]):
    """
    Партиция со степенным распределением.
    Один параметр skew вместо списка весов.

    skew = 1.0 — равномерное распределение
    skew = 2.0 — умеренный перекос к началу (первые значения чаще)
    skew = 3.0+ — сильный перекос
    """
    _specification: ISpecification[T]
    _read_offset: int
    _skew: float
    _values: list[T]
    _value_set: set[T]

    def __init__(self, skew: float, specification: ISpecification):
        self._skew = skew
        self._specification = specification
        self._read_offset = 0
        self._values = []
        self._value_set = set()

    @property
    def read_offset(self):
        return self._read_offset

    @read_offset.setter
    def read_offset(self, val: int):
        self._read_offset = val

    def __contains__(self, value: T):
        return value in self._value_set

    def __len__(self):
        return len(self._values)

    def values(self, offset: int = 0):
        if offset == 0:
            return self._values
        else:
            return self._values[offset:]

    def append(self, value: T, read_offset: int = 0):
        if value not in self._value_set:
            self._values.append(value)
            self._value_set.add(value)
        if read_offset:
            self._read_offset = read_offset

    def remove(self, value: T) -> bool:
        """Удаляет объект из партиции. Возвращает True если объект был удалён."""
        if value not in self._value_set:
            return False
        self._value_set.discard(value)
        self._values.remove(value)
        return True

    def get_relative_position(self, value: T) -> float | None:
        """Возвращает относительную позицию объекта (0.0 - 1.0) или None если не найден."""
        if value not in self._value_set:
            return None
        idx = self._values.index(value)
        n = len(self._values)
        return idx / n if n > 0 else 0.0

    def insert_at_relative_position(self, value: T, relative_position: float) -> None:
        """Вставляет объект в позицию, соответствующую относительной позиции."""
        if value in self._value_set:
            return
        n = len(self._values)
        idx = int(relative_position * n)
        idx = max(0, min(idx, n))
        self._values.insert(idx, value)
        self._value_set.add(value)

    def populate_from(self, source: 'SkewPartition') -> None:
        values_length = len(source)
        if self._read_offset < values_length:
            current_offset = self._read_offset
            self._read_offset = values_length
            for value in source.values(current_offset):
                if self._specification.is_satisfied_by(value):
                    self.append(value, values_length)

    def _select_idx(self) -> int:
        """Выбирает индекс со степенным распределением. O(1)"""
        n = len(self._values)
        # Степенное распределение: idx = n * (1 - random)^skew
        # При skew=1: равномерное (25% в каждом квартиле)
        # При skew=2: перекос к началу (50% в первом квартиле)
        # При skew=3: сильный перекос (63% в первом квартиле)
        idx = int(n * (1 - random.random()) ** self._skew)
        return min(idx, n - 1)

    def next(self, expected_mean: float) -> T:
        """
        Возвращает случайное значение из партиции.
        Бросает StopIteration с вероятностью 1/expected_mean (сигнал создать новое).
        """
        n = len(self._values)
        if n == 0:
            raise StopIteration

        if random.random() < 1.0 / expected_mean:
            raise StopIteration

        return self._values[self._select_idx()]

    def select(self) -> T:
        """Выбор значения без вероятностного отказа (fallback)."""
        n = len(self._values)
        if n == 0:
            raise StopIteration

        return self._values[self._select_idx()]

    def first(self) -> T:
        return self._values[0]


class SkewDistributor(Observable, IM2ODistributor[T], typing.Generic[T]):
    """
    Дистрибьютор со степенным распределением.

    Один параметр skew вместо списка весов:
    - skew = 1.0 — равномерное распределение
    - skew = 2.0 — умеренный перекос (первые 20% получают ~60% вызовов)
    - skew = 3.0 — сильный перекос (первые 10% получают ~70% вызовов)

    Преимущества:
    - O(1) выбор значения (vs O(n) у Distributor)
    - Один параметр вместо списка весов
    - Нет проблемы миграции значений между партициями
    """
    _mean: float = 50
    _partitions: dict[ISpecification, SkewPartition[T]]
    _skew: float
    _default_spec: ISpecification = None
    _provider_name: str | None = None

    def __init__(
            self,
            skew: float = 2.0,
            mean: float | None = None,
    ):
        self._skew = skew
        if mean is not None:
            self._mean = mean
        self._default_spec = EmptySpecification()
        self._partitions = dict()
        self._partitions[self._default_spec] = SkewPartition(self._skew, self._default_spec)
        super().__init__()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        if specification is None:
            specification = EmptySpecification()

        if specification != self._default_spec:
            if specification not in self._partitions:
                self._partitions[specification] = SkewPartition(self._skew, specification)
            target_partition = self._partitions[specification]
            source_partition = self._partitions[self._default_spec]
            target_partition.populate_from(source_partition)

        target_partition = self._partitions[specification]

        if self._mean == 1:
            raise StopAsyncIteration(None)

        try:
            value = target_partition.next(self._mean)
        except StopIteration:
            raise StopAsyncIteration(None)

        # Проверяем, соответствует ли объект спецификации (мог "протухнуть")
        if not specification.is_satisfied_by(value):
            self._relocate_stale_value(value, specification)
            # Retry
            return await self.next(session, specification)

        return value

    def _relocate_stale_value(self, value: T, current_spec: ISpecification[T]) -> None:
        """
        Перемещает протухший объект из текущей партиции в подходящие.
        """
        # Удаляем из текущей партиции
        current_partition = self._partitions.get(current_spec)
        if current_partition:
            current_partition.remove(value)

        # Получаем относительную позицию из default партиции
        default_partition = self._partitions[self._default_spec]
        relative_position = default_partition.get_relative_position(value)

        if relative_position is None:
            return

        # Перебираем все партиции (кроме default и текущей) и вставляем куда подходит
        for spec, partition in self._partitions.items():
            if spec == self._default_spec or spec == current_spec:
                continue
            if spec.is_satisfied_by(value):
                partition.insert_at_relative_position(value, relative_position)

    async def append(self, session: ISession, value: T):
        if value not in self._partitions[self._default_spec]:
            self._partitions[self._default_spec].append(value)
            await self.anotify('value', session, value)
        return

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        if self._provider_name is None:
            self._provider_name = value

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self
