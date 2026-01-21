import typing
# from pydash import predicates

from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor, ISpecification
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.seedwork.domain.utils.data import is_subset, hashable

__all__ = ('ObjectPatternSpecification',)


T = typing.TypeVar("T", covariant=True)


class ObjectPatternSpecification(ISpecification[T], typing.Generic[T]):
    _object_pattern: dict
    _hash: int | None
    _object_exporter: typing.Callable[[T], dict]
    _providers_accessor: typing.Callable[[], dict] | None
    _resolved_pattern: dict | None

    __slots__ = ('_object_pattern', '_object_exporter', '_hash', '_providers_accessor', '_resolved_pattern')

    def __init__(
            self,
            object_pattern: dict,
            object_exporter: typing.Callable[[T], dict],
            providers_accessor: typing.Callable[[], dict] | None = None,
    ):
        self._object_pattern = object_pattern
        self._object_exporter = object_exporter
        self._providers_accessor = providers_accessor
        self._resolved_pattern = None
        self._hash = None

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(hashable(self._object_pattern))
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectPatternSpecification):
            return False
        return hash(self) == hash(other)

    def is_satisfied_by(self, obj: T) -> bool:
        state = self._object_exporter(obj)
        pattern = self._resolved_pattern or self._object_pattern
        # return predicates.is_match(state, self._object_pattern)
        return is_subset(pattern, state)

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_object_pattern_specification(self._object_pattern)

    async def resolve_nested(self, session: ISession) -> None:
        """
        Резолвит вложенные dict constraints в конкретные ID.
        Вызывается дистрибьютором после null-check.

        Args:
            session: сессия для запросов
        """
        if self._resolved_pattern is not None:
            return

        if self._providers_accessor is None:
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

        providers = self._providers_accessor()
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
