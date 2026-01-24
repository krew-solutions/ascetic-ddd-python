"""
Экспериментальная версия. PoC.
"""
import typing

from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor, ISpecification
from ascetic_ddd.seedwork.domain.session import ISession
from ascetic_ddd.seedwork.domain.utils.data import is_subset, hashable

__all__ = ('ObjectPatternLookupSpecification',)


T = typing.TypeVar("T", covariant=True)


class ObjectPatternLookupSpecification(ISpecification[T], typing.Generic[T]):
    """
    Specification с nested lookup в is_satisfied_by().

    В отличие от ObjectPatternResolvableSpecification, не резолвит вложенные constraints
    заранее, а делает lookup при каждой проверке (с кешированием).

    Преимущества:
    - Один индекс на логический паттерн (эффективное индексирование)
    - Новые объекты автоматически учитываются (lookup в момент проверки)

    Недостатки:
    - Распределение nested объектов не учитывается
    - Требует доступ к providers при is_satisfied_by()

    Пример:
        spec = ObjectPatternLookupSpecification(
            {'fk_id': {'status': 'active'}},
            lambda obj: {'fk_id': obj.fk_id},
            providers_accessor=lambda: aggregate_provider
        )
        # Индекс один для всех объектов с active fk
        # is_satisfied_by() проверяет fk.status == 'active' через lookup
    """

    _object_pattern: dict
    _hash: int | None
    _str: str | None
    _object_exporter: typing.Callable[[T], dict]
    _aggregate_provider_accessor: typing.Callable[[], typing.Any] | None
    _nested_cache: dict[tuple[type, str, typing.Any], bool]  # {(provider_type, field_key, fk_id): matches}

    __slots__ = (
        '_object_pattern',
        '_object_exporter',
        '_hash',
        '_str',
        '_aggregate_provider_accessor',
        '_nested_cache',
    )

    def __init__(
            self,
            object_pattern: dict,
            object_exporter: typing.Callable[[T], dict],
            aggregate_provider_accessor: typing.Callable[[], typing.Any] | None = None,
    ):
        self._object_pattern = object_pattern
        self._object_exporter = object_exporter
        self._aggregate_provider_accessor = aggregate_provider_accessor
        self._hash = None
        self._str = None
        self._nested_cache = {}

    def __str__(self) -> str:
        if self._str is None:
            self._str = str(hashable(self._object_pattern))
        return self._str

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(hashable(self._object_pattern))
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectPatternLookupSpecification):
            return False
        return self._object_pattern == other._object_pattern

    async def is_satisfied_by(self, session: ISession, obj: T) -> bool:
        if self._aggregate_provider_accessor is None:
            # Без провайдеров - только простое сравнение
            state = self._object_exporter(obj)
            return is_subset(self._object_pattern, state)

        state = self._object_exporter(obj)
        aggregate_provider = self._aggregate_provider_accessor()
        return await self._matches_pattern_with_provider(
            session, self._object_pattern, state, aggregate_provider
        )

    async def _matches_pattern_with_provider(
            self,
            session: typing.Any,
            pattern: dict,
            state: dict,
            aggregate_provider: typing.Any
    ) -> bool:
        """Проверяет соответствие state паттерну с nested lookup через провайдер."""
        for key, value in pattern.items():
            if isinstance(value, dict):
                # Nested constraint - нужен lookup
                if not await self._matches_nested(session, key, state.get(key), value, aggregate_provider):
                    return False
            else:
                # Simple value comparison
                if state.get(key) != value:
                    return False
        return True

    async def _matches_nested(
            self,
            session: typing.Any,
            field_key: str,
            fk_id: typing.Any,
            nested_pattern: dict,
            aggregate_provider: typing.Any
    ) -> bool:
        """
        Проверяет, удовлетворяет ли связанный объект nested pattern.

        Использует кеш для избежания повторных lookup'ов.

        Args:
            session: сессия для запросов к repository
            field_key: имя поля (ключ для провайдера)
            fk_id: значение foreign key
            nested_pattern: паттерн для проверки связанного объекта
            aggregate_provider: провайдер текущего уровня

        Returns:
            True если связанный объект удовлетворяет паттерну
        """
        if fk_id is None:
            return False

        # Ключ кеша включает тип провайдера для избежания коллизий
        # между одинаковыми field_key на разных уровнях вложенности
        cache_key = (type(aggregate_provider), field_key, fk_id)

        if cache_key in self._nested_cache:
            return self._nested_cache[cache_key]

        # Делаем lookup
        result = await self._do_nested_lookup(session, field_key, fk_id, nested_pattern, aggregate_provider)
        self._nested_cache[cache_key] = result
        return result

    async def _do_nested_lookup(
            self,
            session: typing.Any,
            field_key: str,
            fk_id: typing.Any,
            nested_pattern: dict,
            aggregate_provider: typing.Any
    ) -> bool:
        """
        Выполняет lookup связанного объекта и проверяет паттерн.

        Args:
            session: сессия для запросов к repository
            field_key: имя поля (ключ для провайдера)
            fk_id: значение foreign key
            nested_pattern: паттерн для проверки
            aggregate_provider: провайдер текущего уровня

        Returns:
            True если связанный объект удовлетворяет паттерну
        """
        from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider

        providers = aggregate_provider._providers
        nested_provider = providers.get(field_key)

        if not isinstance(nested_provider, IReferenceProvider):
            # Не reference provider - не можем делать lookup
            return fk_id is not None

        # Получаем связанный объект через repository вложенного агрегата
        referenced_aggregate_provider = nested_provider.aggregate_provider
        repository = referenced_aggregate_provider._repository
        foreign_obj = await repository.get(session, fk_id)

        if foreign_obj is None:
            return False

        # Экспортируем состояние через exporter вложенного агрегата
        foreign_state = referenced_aggregate_provider._output_exporter(foreign_obj)

        # Рекурсивно проверяем nested pattern с провайдером вложенного уровня
        return await self._matches_pattern_with_provider(
            session, nested_pattern, foreign_state, referenced_aggregate_provider
        )

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_object_pattern_specification(
            self._object_pattern,
            self._aggregate_provider_accessor
        )

    def clear_cache(self) -> None:
        """Очищает кеш nested lookup'ов."""
        self._nested_cache.clear()
