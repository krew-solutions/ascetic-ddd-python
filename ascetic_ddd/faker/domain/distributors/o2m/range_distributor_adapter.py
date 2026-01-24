import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor, IM2ODistributorFactory
from ascetic_ddd.faker.domain.distributors.m2o.nullable_distributor import NullableDistributor
from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor
from ascetic_ddd.faker.domain.distributors.o2m.weighted_range_distributor import WeightedRangeDistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.observable.observable import Observable

__all__ = ('RangeDistributorAdapter', 'RangeDistributorFactory')

T = typing.TypeVar("T")


class RangeDistributorAdapter(Observable, IM2ODistributor[T], typing.Generic[T]):
    """
    Адаптер O2M range distributor к интерфейсу IM2ODistributor.

    Использует O2M дистрибьютор для генерации числа из диапазона,
    затем ищет значение по этому числу в словаре.

    Если значение не найдено — бросает Cursor(),
    сигнализируя вызывающему коду создать новое значение для этого слота.

    Пример:
        from ascetic_ddd.faker.domain.distributors.o2m import (
            WeightedRangeDistributor,
            RangeDistributorAdapter,
        )

        range_dist = WeightedRangeDistributor.exponential_decay(0, 99, decay=0.7)
        adapter = RangeDistributorAdapter(range_dist)

        # В provider:
        try:
            company = await adapter.next(session)
        except Cursor as cursor:
            company = await create_company(cursor.position)
            await cursor.append(session, company)
    """
    _distributor: IO2MDistributor
    _values: dict[int, T]
    _provider_name: str | None

    def __init__(self, distributor: IO2MDistributor):
        """
        Args:
            distributor: O2M дистрибьютор, возвращающий числа из диапазона
        """
        self._distributor = distributor
        self._values = {}
        self._provider_name = None
        super().__init__()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        """
        Возвращает значение из словаря по случайному числу.

        Raises:
            Cursor(): если для слота нет значения в словаре.
            cursor.position — номер слота.
            cursor.append(session, value) — добавить значение.
        """
        num = self._distributor.distribute()

        if num not in self._values:
            raise Cursor(
                position=num,
                callback=self._append,
            )

        value = self._values[num]

        # Проверяем спецификацию если указана
        if specification is not None and not await specification.is_satisfied_by(session, value):
            # Значение не подходит под спецификацию — пробуем ещё раз
            return await self.next(session, specification)

        return value

    async def _append(self, session: ISession, value: T, position: int | None = None):
        """
        Добавляет значение в словарь.

        Args:
            session: Сессия
            value: Значение для добавления
            position: Номер слота (ключ в словаре).
        """
        self._values[position] = value
        await self.anotify('value', session, value)

    async def append(self, session: ISession, value: T):
        await self._append(session, value, None)

    @property
    def provider_name(self) -> str | None:
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value: str):
        if self._provider_name is None:
            self._provider_name = value

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        self._values.clear()

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def __len__(self) -> int:
        """Количество значений в словаре."""
        return len(self._values)

    def __contains__(self, value: T) -> bool:
        """Проверяет наличие значения в словаре."""
        return value in self._values.values()


class RangeDistributorFactory(IM2ODistributorFactory[T], typing.Generic[T]):
    """
    Фабрика M2O дистрибьюторов на основе диапазона.

    Создаёт RangeDistributorAdapter с WeightedRangeDistributor внутри.

    Пример:
        factory = RangeDistributorFactory(min_val=0, max_val=99)

        # Создать дистрибьютор с весами
        dist = factory(weights=[0.7, 0.2, 0.1])

        # Создать дистрибьютор со skew (exponential decay)
        dist = factory(skew=2.0)

        # Использование
        try:
            value = await dist.next(session)
        except ICursor as cursor:
            new_value = create_value(cursor.position)
            await cursor.append(session, new_value)
    """
    _min_val: int
    _max_val: int

    def __init__(self, min_val: int, max_val: int):
        """
        Args:
            min_val: Минимальное значение диапазона (включительно)
            max_val: Максимальное значение диапазона (включительно)
        """
        self._min_val = min_val
        self._max_val = max_val

    def __call__(
        self,
        weights: list[float] | None = None,
        skew: float | None = None,
        mean: float | None = None,
        null_weight: float = 0,
        sequence: bool = False,
    ) -> IM2ODistributor[T]:
        """
        Создаёт M2O дистрибьютор.

        Args:
            weights: Веса для каждого значения в диапазоне
            skew: Параметр перекоса для exponential decay (decay = 1/skew)
            mean: Не используется (для совместимости с интерфейсом)
            null_weight: Вероятность вернуть None (0-1)
            sequence: Не используется (для совместимости с интерфейсом)

        Returns:
            RangeDistributorAdapter с соответствующим WeightedRangeDistributor
        """
        if weights is not None:
            range_dist = WeightedRangeDistributor(
                self._min_val,
                self._max_val,
                weights=weights,
            )
        elif skew is not None and skew > 1:
            # skew -> decay: чем больше skew, тем меньше decay
            decay = 1.0 / skew
            range_dist = WeightedRangeDistributor.exponential_decay(
                self._min_val,
                self._max_val,
                decay=decay,
            )
        else:
            # Равномерное распределение
            range_dist = WeightedRangeDistributor.uniform(
                self._min_val,
                self._max_val,
            )

        adapter = RangeDistributorAdapter(range_dist)

        if null_weight > 0:
            return NullableDistributor(adapter, null_weight=null_weight)

        return adapter
