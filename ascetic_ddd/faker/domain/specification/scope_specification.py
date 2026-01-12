import typing
from typing import Hashable

from .interfaces import ISpecification, ISpecificationVisitor


__all__ = ("ScopeSpecification",)

T = typing.TypeVar("T", covariant=True)


class ScopeSpecification(ISpecification[T], typing.Generic[T]):
    _scope: Hashable
    _hash: int | None

    __slots__ = ('_scope', '_hash')

    def __init__(self, scope: Hashable):
        self._scope = scope
        self._hash = None

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(self._scope)
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ScopeSpecification):
            return False
        return self._scope == other._scope

    def is_satisfied_by(self, obj: T) -> bool:
        return True

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_scope_specification(self._scope)
