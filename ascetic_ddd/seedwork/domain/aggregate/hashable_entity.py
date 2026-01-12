from abc import ABCMeta, abstractmethod

from ....specification.domain.interfaces import IEqualOperand
from .interfaces import IHashable

__all__ = ("HashableEntity",)


class HashableEntity(IHashable, metaclass=ABCMeta):
    @property
    @abstractmethod
    def id(self) -> IEqualOperand:  # noqa: A003 # id shadowing Python builtin
        """
        See also IsTransient
        https://github.com/dotnet-architecture/eShopOnContainers/blob/dev/src/Services/Ordering/Ordering.Domain/SeedWork/Entity.cs#L42.
        """
        raise NotImplementedError

    def __hash__(self):
        id_ = self.id
        if id_ is None:
            raise TypeError("Model instances without primary key value are unhashable")
        return hash(id_)

    def __eq__(self, other: IEqualOperand):
        assert isinstance(other, HashableEntity)
        return self.id == other.id
