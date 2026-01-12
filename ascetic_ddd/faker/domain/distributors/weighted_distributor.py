import math
import random
import typing

from ascetic_ddd.faker.domain.distributors.interfaces import IDistributor
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.observable.observable import Observable

__all__ = ('WeightedDistributor',)

T = typing.TypeVar("T", covariant=True)


class Partition(typing.Generic[T]):
    _specification: ISpecification[T]
    _read_offset: int
    _weights: list[float]
    _values: list[T]
    _value_set: set[T]

    def __init__(self, weights: list[float], specification: ISpecification[T]):
        self._weights = weights
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

    def populate_from(self, source: 'Partition') -> None:
        values_length = len(source)
        if self._read_offset < values_length:
            current_offset = self._read_offset
            self._read_offset = values_length
            for value in source.values(current_offset):
                if self._specification.is_satisfied_by(value):
                    self.append(value, values_length)

    def _select_idx(self) -> int:
        """Выбирает индекс значения с учётом весов и наклона."""
        n = len(self._values)

        # Выбираем партицию по весам — O(w)
        partition_idx = random.choices(
            range(len(self._weights)),
            weights=self._weights,
            k=1
        )[0]

        # Вычисляем границы партиции — O(1)
        partition_size = n / len(self._weights)
        start = int(partition_idx * partition_size)
        end = int((partition_idx + 1) * partition_size)
        if end <= start:
            end = start + 1
        end = min(end, n)

        # Вычисляем локальный наклон из соотношения весов соседних партиций — O(1)
        # Используем ЛЕВУЮ партицию и смещаем к КОНЦУ — это компенсирует то, что ранние
        # значения получают больше вызовов (доступны дольше при динамическом создании).
        # Для weights=[0.7, 0.2, 0.07, 0.03]:
        #   partition 0: первая → local_skew=1.0 (равномерно)
        #   partition 1: ratio=3.5 → local_skew≈2.81 (смещение к концу, ближе к partition 0)
        #   partition 2: ratio=2.86 → local_skew≈2.52
        #   partition 3: ratio=2.33 → local_skew≈2.22
        if partition_idx > 0:
            prev_weight = self._weights[partition_idx - 1]
            curr_weight = self._weights[partition_idx]
            if curr_weight > 0:
                ratio = prev_weight / curr_weight
                # ratio > 1 → смещение к концу партиции (ближе к предыдущей)
                # ratio = 1 → равномерное распределение
                local_skew = max(1.0, math.log2(ratio) + 1)
                # local_skew = max(1.0, math.log2(ratio) * 0.5 + 1)  # более плавный наклон
            else:
                local_skew = 2.0
        else:
            # Первая партиция — равномерное распределение
            local_skew = 1.0

        # Выбираем значение с учётом наклона — O(1)
        # Смещение к КОНЦУ партиции: end - 1 - local_idx
        size = end - start
        local_idx = int(size * (1 - random.random()) ** local_skew)
        local_idx = min(local_idx, size - 1)
        return end - 1 - local_idx

    def next(self, expected_scale: float) -> T:
        """
        Возвращает случайное значение из партиции.
        Бросает StopIteration с вероятностью 1/expected_scale (сигнал создать новое).
        """
        n = len(self._values)
        if n == 0:
            raise StopIteration

        # Вероятностно сигнализируем о необходимости создать новое значение
        if random.random() < 1.0 / expected_scale:
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


class WeightedDistributor(Observable, IDistributor[T], typing.Generic[T]):
    """
    weights should be one of:
    - sum of partition divided by total sum
    - max in partition divided by total max
    Если использовать max, тогда первое значение всегда будет равно единице, что неудобно в использовании.
    Поэтому выбираем sum.

    Если sum(self._weights) >= 1, тогда пополнение будет через параметр self._scale.
    Если sum(self._weights) < 1, тогда пополнение будет без учета параметра self._scale,
    т.к. ни одна партиция не сможет достигнуть своего веса.

    В перспективе можно перейти на функциональную выборку, см.
    https://dataschool.com/learn-sql/random-sequences/
    Достаточно поддерживать три вида функции:
    - линейная
    - экспоненциальная
    - логарифмическая
    """
    _scale: float = 50
    _partitions: dict[ISpecification, Partition[T]]
    _weights: list[float]
    _default_spec: ISpecification = None
    _provider_name: str | None = None

    def __init__(
            self,
            weights: typing.Iterable[float] = tuple(),
            scale: float | None = None,
    ):
        self._weights = list(weights)
        if scale is not None:
            self._scale = scale
        self._default_spec = EmptySpecification()
        self._partitions = dict()
        self._partitions[self._default_spec] = Partition(self._weights, self._default_spec)
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
                self._partitions[specification] = Partition(self._weights, specification)
            target_partition = self._partitions[specification]
            source_partition = self._partitions[self._default_spec]
            target_partition.populate_from(source_partition)

        target_partition = self._partitions[specification]

        if self._scale == 1:
            raise StopAsyncIteration(None)

        try:
            return target_partition.next(self._scale)
        except StopIteration:
            raise StopAsyncIteration(None)

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
