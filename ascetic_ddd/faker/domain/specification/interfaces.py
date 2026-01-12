import typing
from abc import ABCMeta, abstractmethod

__all__ = ('ISpecificationVisitor', 'ISpecificationVisitable', 'ISpecification',)

T = typing.TypeVar("T", covariant=True)


class ISpecificationVisitor(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def visit_object_pattern_specification(self, object_pattern: typing.Any):
        raise NotImplementedError

    @abstractmethod
    def visit_empty_specification(self):
        raise NotImplementedError

    @abstractmethod
    def visit_scope_specification(self, scope: typing.Hashable):
        raise NotImplementedError


class ISpecificationVisitable(typing.Protocol[T], metaclass=ABCMeta):

    @abstractmethod
    def accept(self, visitor: ISpecificationVisitor):
        raise NotImplementedError


class ISpecification(ISpecificationVisitable[T], typing.Protocol[T], metaclass=ABCMeta):

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        raise NotImplementedError

    @abstractmethod
    def is_satisfied_by(self, obj: T) -> bool:
        raise NotImplementedError
