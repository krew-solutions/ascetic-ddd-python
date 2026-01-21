import typing

__all__ = ('ISpecificationVisitor', 'ISpecificationVisitable', 'ISpecification',)

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

    def __hash__(self) -> int:
        ...

    def __eq__(self, other: object) -> bool:
        ...

    def is_satisfied_by(self, obj: T) -> bool:
        ...
