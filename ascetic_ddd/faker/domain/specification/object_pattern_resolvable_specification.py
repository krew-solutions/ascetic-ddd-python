import typing
# from pydash import predicates

from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor, IResolvableSpecification
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.seedwork.domain.utils.data import is_subset, hashable

__all__ = ('ObjectPatternResolvableSpecification',)


T = typing.TypeVar("T", covariant=True)


class ObjectPatternResolvableSpecification(IResolvableSpecification[T], typing.Generic[T]):
    _object_pattern: dict
    _hash: int | None
    _str: str | None
    _object_exporter: typing.Callable[[T], dict]
    _aggregate_provider_accessor: typing.Callable[[], typing.Any] | None
    _resolved_pattern: dict | None

    __slots__ = ('_object_pattern', '_object_exporter', '_hash', '_aggregate_provider_accessor', '_resolved_pattern')

    def __init__(
            self,
            object_pattern: dict,
            object_exporter: typing.Callable[[T], dict],
            aggregate_provider_accessor: typing.Callable[[], typing.Any] | None = None,
    ):
        self._object_pattern = object_pattern
        self._object_exporter = object_exporter
        self._aggregate_provider_accessor = aggregate_provider_accessor
        self._resolved_pattern = None
        self._hash = None
        self._str = None

    def __str__(self) -> str:
        if self._resolved_pattern is None:
            raise TypeError(
                "Cannot cast to string unresolved ObjectPatternResolvableSpecification. "
                "Call resolve_nested() first."
            )
        if self._str is None:
            self._str = str(hashable(self._resolved_pattern))
        return self._str

    def __hash__(self) -> int:
        if self._resolved_pattern is None:
            raise TypeError(
                "Cannot hash unresolved ObjectPatternResolvableSpecification. "
                "Call resolve_nested() first."
            )
        if self._hash is None:
            self._hash = hash(hashable(self._resolved_pattern))
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectPatternResolvableSpecification):
            return False
        # Оба должны быть resolved для сравнения
        if self._resolved_pattern is None or other._resolved_pattern is None:
            raise TypeError(
                "Cannot compare unresolved ObjectPatternResolvableSpecification. "
                "Call resolve_nested() first."
            )
        return self._resolved_pattern == other._resolved_pattern

    async def is_satisfied_by(self, session: ISession, obj: T) -> bool:
        if self._resolved_pattern is None:
            raise TypeError(
                "Cannot use unresolved ObjectPatternResolvableSpecification. "
                "Call resolve_nested() first."
            )
        state = self._object_exporter(obj)
        # return predicates.is_match(state, self._object_pattern)
        return is_subset(self._resolved_pattern, state)

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_object_pattern_specification(
            self._resolved_pattern or self._object_pattern,
            self._aggregate_provider_accessor
        )

    async def resolve_nested(self, session: ISession) -> None:
        """
        Резолвит вложенные dict constraints в конкретные ID.
        Вызывается дистрибьютором после null-check.

        Args:
            session: сессия для запросов
        """
        if self._resolved_pattern is not None:
            return

        if self._aggregate_provider_accessor is None:
            self._resolved_pattern = self._object_pattern
            return

        self._resolved_pattern = await self._do_resolve_nested(session)

    async def _do_resolve_nested(self, session: ISession) -> dict:
        """Depth-first resolution: рекурсивно резолвит вложенные dict в конкретные ID.

        {'fk_id': {'nested_fk': {'status': 'active'}}}
        → сначала резолвит nested_fk с status='active'
        → потом возвращает {'fk_id': <конкретный ID>}
        """
        from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider

        aggregate_provider = self._aggregate_provider_accessor()
        providers = aggregate_provider._providers
        resolved = {}

        for key, value in self._object_pattern.items():
            if isinstance(value, dict):
                nested_provider = providers.get(key)
                if isinstance(nested_provider, IReferenceProvider):
                    nested_provider.set(value)
                    await nested_provider.populate(session)
                    resolved[key] = nested_provider.get()
                else:
                    resolved[key] = value
            else:
                resolved[key] = value

        return resolved
