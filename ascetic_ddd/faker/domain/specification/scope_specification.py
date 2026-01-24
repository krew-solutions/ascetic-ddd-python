import typing
from typing import Hashable

from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.seedwork.domain.utils.data import hashable
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification, ISpecificationVisitor


__all__ = ("ScopeSpecification",)

T = typing.TypeVar("T", covariant=True)


class ScopeSpecification(ISpecification[T], typing.Generic[T]):
    _scope: Hashable
    _hash: int | None
    _str: str | None

    __slots__ = ('_scope', '_hash')

    def __init__(self, scope: Hashable):
        self._scope = scope
        self._hash = None
        self._str = None

    def __str__(self) -> str:
        if self._str is None:
            self._str = str(hashable(self._scope))
        return self._str

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(hashable(self._scope))
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ScopeSpecification):
            return False
        return self._scope == other._scope

    async def is_satisfied_by(self, session: ISession, obj: T) -> bool:
        return True

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_scope_specification(self._scope)
