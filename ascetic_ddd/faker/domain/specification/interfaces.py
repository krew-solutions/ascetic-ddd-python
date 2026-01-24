import typing

from ascetic_ddd.seedwork.domain.session.interfaces import ISession


__all__ = (
    'ISpecificationVisitor',
    'ISpecificationVisitable',
    'ISpecification',
    'IResolvableSpecification',
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


class ISpecification(ISpecificationVisitable[T], typing.Protocol[T]):

    def __str__(self) -> str:
        ...

    def __hash__(self) -> int:
        ...

    def __eq__(self, other: object) -> bool:
        ...

    async def is_satisfied_by(self, session: ISession, obj: T) -> bool:
        ...


class IResolvableSpecification(ISpecification[T], typing.Protocol[T]):
    """Интерфейс для specification, требующего pre-resolve."""

    async def resolve_nested(self, session: ISession) -> None:
        ...
