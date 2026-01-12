import typing
# from pydash import predicates

from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor, ISpecification
from ascetic_ddd.seedwork.domain.utils.data import is_subset, hashable

__all__ = ('ObjectPatternSpecification',)


T = typing.TypeVar("T", covariant=True)


class ObjectPatternSpecification(ISpecification[T], typing.Generic[T]):
    _object_pattern: dict
    _hash: int | None

    __slots__ = ('_object_pattern', '_state_accessor', '_hash')

    def __init__(self, object_pattern: dict, state_accessor: typing.Callable[[T], dict]):
        self._object_pattern = object_pattern
        self._state_accessor = state_accessor
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
        state = self._state_accessor(obj)
        # return predicates.is_match(state, self._object_pattern)
        return is_subset(self._object_pattern, state)

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_object_pattern_specification(self._object_pattern)
