import typing

from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor, ISpecification

__all__ = ("EmptySpecification",)


class EmptySpecification(ISpecification):

    __slots__ = ('_hash',)

    def __init__(self):
        self._hash: int | None = None

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset())
        return self._hash

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EmptySpecification)

    def is_satisfied_by(self, obj: typing.Any) -> bool:
        return True

    def accept(self, visitor: ISpecificationVisitor):
        visitor.visit_empty_specification()
