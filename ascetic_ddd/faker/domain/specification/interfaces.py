import typing

from ascetic_ddd.faker.domain.session.interfaces import ISession


__all__ = (
    'ISpecificationVisitor',
    'ISpecificationVisitable',
    'IBaseSpecification',
    'ISpecification',
    'ILookupSpecification',
)

T = typing.TypeVar("T", covariant=True)


class ISpecificationVisitor(typing.Protocol):

    def visit_object_pattern_specification(
            self,
            object_pattern: typing.Any,
            aggregate_provider_accessor: typing.Callable[[], typing.Any] | None = None
    ):
        ...

    def visit_empty_specification(self):
        ...

    def visit_scope_specification(self, scope: typing.Hashable):
        ...


class ISpecificationVisitable(typing.Protocol[T]):

    def accept(self, visitor: ISpecificationVisitor):
        ...


class IBaseSpecification(ISpecificationVisitable[T], typing.Protocol[T]):

    def __str__(self) -> str:
        ...

    def __hash__(self) -> int:
        ...

    def __eq__(self, other: object) -> bool:
        ...


class ISpecification(IBaseSpecification[T], typing.Protocol[T]):

    def is_satisfied_by(self, obj: T) -> bool:
        ...


class ILookupSpecification(IBaseSpecification[T], typing.Protocol[T]):

    async def is_satisfied_by(self, session: ISession, obj: T) -> bool:
        ...
